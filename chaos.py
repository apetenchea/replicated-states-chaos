import logging
import time
import psutil
import json
import string
import random
import httpx
import argparse
from multiprocessing import Pool, Process


STATE_ID = 12
WC = 3
RC = 5
COORD_URL = f'http://localhost:8530'
REPLICATED_LOG_URL = f'_api/log/{STATE_ID}'
REPLICATED_STATE_URL = '_api/replicated-state'
PROTOTYPE_STATE_URL = f'_api/prototype-state/{STATE_ID}'


logging.basicConfig(level=logging.INFO)


def kill_all():
    for proc in psutil.process_iter(['name']):
        if proc.name() == 'arangod':
            proc.kill()


def resume_all():
    for proc in psutil.process_iter(['name']):
        if proc.name() == 'arangod':
            proc.resume()


def create_prototype_state():
    state_type = 'prototype'
    config = {
        'id': STATE_ID,
        'config': {
            'waitForSync': False,
            'writeConcern': WC,
            'softWriteConcern': WC,
            'replicationFactor': RC,
        },
        'properties': {
            "implementation": {"type": state_type}
        }
    }
    r = httpx.post(f'{COORD_URL}/{REPLICATED_STATE_URL}', json=config)
    r.raise_for_status()


def get_endpoints():
    url = f'{COORD_URL}/_admin/cluster/health'
    r = httpx.get(url)
    if r.is_success:
        s = r.json()
        prmr = {p: v['Endpoint'] for p, v in s['Health'].items() if p.startswith('PRMR')}
        return prmr
    else:
        r.raise_for_status()


def get_pid_by_endpoint(endpoint):
    url = f'http://localhost:{endpoint.split(":")[-1]}'
    response = httpx.get(f'{url}/_admin/status')
    return int(response.json()['pid'])


def get_rlog_participants():
    r = httpx.get(f'{COORD_URL}/{REPLICATED_LOG_URL}')
    if r.is_error:
        r.raise_for_status()
    status = r.json()
    return list(status['result']['participants'].keys())


class Server:
    def __init__(self, name, pid):
        self._name = name
        self._proc = psutil.Process(pid)
        self._is_running = True

    @property
    def name(self):
        return self._name

    @property
    def is_running(self):
        return self._is_running

    def touch(self):
        if self._is_running:
            self._proc.suspend()
            self._is_running = False
            logging.info(f'suspended {self.name}')
        else:
            self._proc.resume()
            self._is_running = True
            logging.info(f'resumed {self.name}')

    def alive(self):
        if not self._is_running:
            self._proc.resume()
            self._is_running = True


def get_participants():
    endpoints = get_endpoints()
    rlog = get_rlog_participants()

    used, unused = [], []
    for k, v in endpoints.items():
        participant = Server(k, get_pid_by_endpoint(v))
        if k in rlog:
            used.append(participant)
        else:
            unused.append(participant)
    return used, unused


def run_participants_chaos():
    used, unused = get_participants()
    while True:
        running = sum(1 for p in used if p.is_running)
        if running < WC:
            op = 1
        else:
            op = random.randint(0, 4)
        if op == 0:
            idx = random.randint(0, len(used) - 1)
            used[idx].touch()
            time.sleep(3)
        elif op == 1 and len(unused):
            new_idx = random.randint(0, len(unused) - 1)
            new_participant = unused[new_idx]
            new_participant.alive()
            old_idx = random.randint(0, len(used) - 1)
            old_participant = used[old_idx]
            while True:
                url = f'{COORD_URL}/{REPLICATED_STATE_URL}/{STATE_ID}/participant/{old_participant.name}/' \
                      f'replace-with/{new_participant.name}'
                r = httpx.post(url)
                if r.is_error:
                    logging.debug(f'Replacing {old_participant.name} with {new_participant.name}'
                                  f'returned {r.status_code}, text {r.text}')
                    time.sleep(3)
                    continue
                logging.info(f'exchanged {old_participant.name} {new_participant.name}')
                unused[new_idx], used[old_idx] = used[old_idx], unused[new_idx]
                break
        else:
            # nop
            pass
        time.sleep(1)


def set_initial_state(entries_range):
    entries = {str(it): '*' for it in range(entries_range)}
    r = httpx.post(f'{COORD_URL}/{PROTOTYPE_STATE_URL}/insert?waitForApplied=true', json=entries)
    if r.is_error:
        logging.error(f'Inserting {entries} resulted in status code {r.status_code}, text {r.text}')
        return None
    return entries


def get_snapshot(last_applied):
    r = httpx.get(f'{COORD_URL}/{PROTOTYPE_STATE_URL}/snapshot?waitForApplied={last_applied}')
    if r.is_error:
        logging.error(f'Snapshot resulted in status code {r.status_code}, text {r.text}')
        return None
    snapshot = r.json()['result']
    return snapshot


def reconstruct_state(initial_state, operations):
    idx = set()
    for o in operations:
        idx.add(o[0])
        payload = o[1]
        key = list(payload.keys())[0]
        if initial_state[key] != payload[key]['oldValue']:
            logging.error(f'During entry {o[0]}, previous state for {key} was {initial_state[key]},'
                          f'but {payload[key]["oldValue"]} was used as old value. New value applied: {payload[key]["newValue"]}')
        initial_state[key] = payload[key]['newValue']
    assert(len(idx) == len(operations))
    return initial_state


class Chaos:
    def __init__(self, name):
        self.name = name
        self.op = list()

    def compare_exchange(self, key):
        while True:
            r = httpx.get(f'{COORD_URL}/{PROTOTYPE_STATE_URL}/entry/{key}', timeout=120)
            if r.is_error:
                logging.debug(f'Getting key {key} returned {r.status_code}, text {r.text}')
                time.sleep(1)
                continue
            old_value = r.json()['result'][key]
            payload = {key: {'newValue': f'{old_value}{self.name}', 'oldValue': old_value}}
            r = httpx.put(f'{COORD_URL}/{PROTOTYPE_STATE_URL}/cmp-ex', json=payload, timeout=120)
            if r.is_error:
                logging.debug(f'Compare-exchange on {key} returned {r.status_code}, text {r.text}')
                time.sleep(1)
                continue
            idx = r.json()['result']['index']
            self.op.append((idx, payload))
            logging.info(f'Applied entry {idx}')
            break

    @classmethod
    def run(cls, name, entries_range, operations_per_thread):
        chaos = cls(name)
        for idx in range(operations_per_thread):
            key = str(random.randint(0, entries_range - 1))
            chaos.compare_exchange(key)
        return chaos.op


def main(args):
    create_prototype_state()
    time.sleep(2)
    entries = set_initial_state(args.entries_range)
    if entries is None:
        logging.error("Could not set initial state!")
        return

    initial_state = get_snapshot(0)
    if initial_state is None:
        logging.error("Could not get initial state!")
        return

    names = [(letter, args.entries_range, args.operations_thread) for letter in string.ascii_uppercase[:args.proc]]
    operations = list()
    p = Process(target=run_participants_chaos)
    p.start()
    with Pool(processes=args.proc) as pool:
        for proc_result in pool.starmap(Chaos.run, names):
            operations.extend(proc_result)
    p.terminate()
    resume_all()

    operations.sort()
    expected = reconstruct_state(initial_state, operations)

    last_index = operations[-1][0]
    actual = get_snapshot(last_index)
    if actual is None:
        logging.error("Could not get latest state!")
        return

    if args.kill:
        kill_all()

    with open('expected.json', 'w') as f:
        json.dump(expected, f)
    with open('actual.json', 'w') as f:
        json.dump(actual, f)
    assert(expected == actual)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prototype State Stress Testing')
    parser.add_argument('--proc', '-p', type=int, default=8, help='number of concurrent processes')
    parser.add_argument('--kill', '-k', action='store_true', help='kill all arangod processes after stopping')
    parser.add_argument('--entries-range', '-er', type=int, default=100, help='number of entries')
    parser.add_argument('--operations-thread', '-op', type=int, default=1000, help='number of requests made by each thread')
    main(parser.parse_args())
