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
WC = 3  # write concern
RC = 8  # number of servers being used to run the replicated state
COORD_URL = ['http://localhost:8530', 'http://localhost:8531', 'http://localhost:8532']
REPLICATED_LOG_URL = f'_api/log/{STATE_ID}'
REPLICATED_STATE_URL = '_api/replicated-state'
PROTOTYPE_STATE_URL = f'_api/prototype-state/{STATE_ID}'
PROCESS_DETAILED_OUTPUT = True


logging.basicConfig(level=logging.INFO)


def coord_url():
    return COORD_URL[random.randint(0, 2)]


def kill_all():
    for proc in psutil.process_iter(['name']):
        if proc.name() == 'arangod':
            proc.kill()


def resume_all():
    for proc in psutil.process_iter(['name']):
        if proc.name() == 'arangod':
            proc.resume()


def create_prototype_state():
    endpoints = get_endpoints()

    state_type = 'prototype'
    config = {
        'id': STATE_ID,
        'config': {
            'waitForSync': False,
            'writeConcern': WC,
            'softWriteConcern': WC,
        },
        'properties': {
            "implementation": {"type": state_type}
        },
        'participants': {p: {} for p in list(endpoints.keys())[:RC]},
    }
    r = httpx.post(f'{coord_url()}/{REPLICATED_STATE_URL}', json=config)
    r.raise_for_status()


def get_endpoints():
    url = f'{coord_url()}/_admin/cluster/health'
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
    r = httpx.get(f'{coord_url()}/{REPLICATED_LOG_URL}')
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
            op = random.randint(0, 2)
        if op == 0:
            idx = random.randint(0, len(used) - 1)
            used[idx].touch()
            time.sleep(5)
        elif op == 1:
            if len(unused):
                new_idx = random.randint(0, len(unused) - 1)
                new_participant = unused[new_idx]
                new_participant.alive()
                old_idx = random.randint(0, len(used) - 1)
                old_participant = used[old_idx]
                while True:
                    url = f'{coord_url()}/{REPLICATED_STATE_URL}/{STATE_ID}/participant/{old_participant.name}/' \
                          f'replace-with/{new_participant.name}'
                    r = httpx.post(url)
                    if r.is_error:
                        logging.debug(f'Replacing {old_participant.name} with {new_participant.name}'
                                      f'returned {r.status_code}, text {r.text}')
                        time.sleep(1)
                        continue
                    logging.info(f'exchanged {old_participant.name} {new_participant.name}')
                    unused[new_idx], used[old_idx] = used[old_idx], unused[new_idx]
                    time.sleep(5)
                    break
            else:
                # We don't have enough servers to continue and no servers to replace the stopped ones.
                # In this case we try to resume one that's currently being in use.
                for p in used:
                    if not p.is_running:
                        p.touch()
                        break
        else:
            # nop
            pass
        time.sleep(1)


def set_initial_state(entries_range):
    entries = {str(it): '*' for it in range(entries_range)}
    r = httpx.post(f'{coord_url()}/{PROTOTYPE_STATE_URL}/insert?waitForApplied=true', json=entries)
    if r.is_error:
        logging.error(f'Inserting {entries} resulted in status code {r.status_code}, text {r.text}')
        return None
    return r.json()['result']['index'], entries


def get_snapshot(last_applied):
    r = httpx.get(f'{coord_url()}/{PROTOTYPE_STATE_URL}/snapshot?waitForApplied={last_applied}')
    if r.is_error:
        logging.error(f'Snapshot resulted in status code {r.status_code}, text {r.text}')
        return None
    snapshot = r.json()['result']
    return snapshot


def get_log_tail():
    r = httpx.get(f'{coord_url()}/{REPLICATED_LOG_URL}/tail?limit=1000')
    if r.is_error:
        logging.debug(f'Log tail resulted in status code {r.status_code}, text {r.text}')
        return None
    tail = r.json()['result']
    return tail


def get_log_entries():
    r = httpx.get(f'{coord_url()}/{REPLICATED_LOG_URL}/head?limit=1000000')
    if r.is_error:
        logging.debug(f'Log head resulted in status code {r.status_code}, text {r.text}')
        return None
    head = r.json()['result']
    return head


def rolling_tail(running, log_tail):
    while running.value:
        tail = get_log_tail() or []
        log_tail.extend(tail)
        time.sleep(0.5)


def trim_tail(log_tail):
    trimmed = []
    for entry in sorted(log_tail, key=lambda x: x['logIndex']):
        if not trimmed or trimmed[-1]['logIndex'] < entry['logIndex']:
            trimmed.append(entry)
    return trimmed


def reconstruct_state_locally(initial_state, operations):
    idx = list()
    for o in operations:
        idx.append(o[0])
        if len(idx) > 1 and idx[-1] != idx[-2] + 1:
            if idx[-1] == idx[-2] + 2:
                logging.error(f'During entry {idx[-1]}, previous recorded entry is {idx[-2]}, most probably a meta-entry')
            else:
                logging.error(f'During entry {idx[-1]}, previous recorded entry is {idx[-2]}')
        payload = o[1]
        key = list(payload.keys())[0]
        if initial_state[key] != payload[key]['oldValue']:
            logging.error(f'During entry {o[0]}, previous state for {key} was {initial_state[key]},'
                          f'but {payload[key]["oldValue"]} was used as old value. New value applied:'
                          f'{payload[key]["newValue"]}. This is '
                          f'{"ok, because the same process sent multiple requests" if payload[key]["multiple"] else "NOT OK"}')
        initial_state[key] = payload[key]['newValue']
    assert(len(set(idx)) == len(operations))
    return initial_state


def reconstruct_state_from_log(entries):
    """
    This works properly when compaction is disabled.
    """
    state = {}
    idx = 0
    for e in entries:
        try:
            payload = e['payload']
        except KeyError:
            idx = e['logIndex']
            continue
        assert(e['logIndex'] == idx + 1)
        idx = e['logIndex']

        op_type = payload[1]['type']
        if op_type == 'Insert':
            state.update(payload[1]['op']['map'])
        elif op_type == 'CompareExchange':
            key = payload[1]['op']['key']
            old_value = payload[1]['op']['oldValue']
            new_value = payload[1]['op']['newValue']
            if state[key] != old_value:
                logging.error(f'During index {idx}: {state[key]} != {old_value}, to be replace by {new_value}')
            state[key] = new_value
    return state


class Chaos:
    def __init__(self, name):
        self.name = name
        self.op = list()
        if PROCESS_DETAILED_OUTPUT:
            self.f = open(f'{self.name}.txt', 'w')

    def __del__(self):
        if PROCESS_DETAILED_OUTPUT:
            self.f.close()

    def compare_exchange(self, key):
        previous = None
        while True:
            r = httpx.get(f'{coord_url()}/{PROTOTYPE_STATE_URL}/entry/{key}', timeout=120)
            if r.is_error:
                if PROCESS_DETAILED_OUTPUT:
                    print(f'Getting key {key} returned {r.status_code}, text {r.text}', file=self.f)
                time.sleep(1)
                continue
            old_value = r.json()['result'][key]
            payload = {key: {'newValue': f'{old_value}{self.name}', 'oldValue': old_value, 'name': self.name,
                             'multiple': False}}
            if previous:
                diff = len(old_value) - len(previous)
                if diff and previous + self.name * diff == old_value:
                    # We already had at least one successful cmp-ex, but didn't get the answer back
                    payload[key]['multiple'] = True
            previous = old_value
            r = httpx.put(f'{coord_url()}/{PROTOTYPE_STATE_URL}/cmp-ex', json=payload, timeout=120)
            if r.is_error:
                if PROCESS_DETAILED_OUTPUT:
                    print(f'Compare-exchange on {key} returned {r.status_code}, text {r.text}', file=self.f)
                continue
            idx = r.json()['result']['index']
            self.op.append((idx, payload))
            logging.info(f'Applied entry {idx}')
            if PROCESS_DETAILED_OUTPUT:
                print(f'Applied entry {idx} on {key}', file=self.f)
            break

    @classmethod
    def run(cls, name, entries_range, operations_per_thread):
        chaos = cls(name)
        for idx in range(operations_per_thread):
            key = str(random.randint(0, entries_range - 1))
            chaos.compare_exchange(key)
        return chaos.op


def dump_log():
    r = httpx.get(f'{coord_url()}/{REPLICATED_LOG_URL}/head?limit=10000000')
    if r.is_error:
        logging.error(f"Could not dump replicated log {r}")
        return
    with open("log.json", "w") as f:
        json.dump(r.json()["result"], f)


def main(args):
    create_prototype_state()
    time.sleep(2)
    idx, entries = set_initial_state(args.entries_range)
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
    last_index = operations[-1][0]

    expected = reconstruct_state_locally(initial_state, operations)

    actual = get_snapshot(last_index)
    if actual is None:
        logging.error("Could not get latest state!")
        return

    if args.kill:
        kill_all()

    dump_log()
    with open('expected.json', 'w') as f:
        json.dump(expected, f)
    with open('actual.json', 'w') as f:
        json.dump(actual, f)
    assert(expected == actual)
    logging.info('Reconstructed states are equal!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prototype State Stress Testing')
    parser.add_argument('--proc', '-p', type=int, default=8, help='number of concurrent processes')
    parser.add_argument('--kill', '-k', action='store_true', help='kill all arangod processes after stopping')
    parser.add_argument('--entries-range', '-er', type=int, default=100, help='number of entries')
    parser.add_argument('--operations-thread', '-op', type=int, default=1000, help='number of requests made by'
                                                                                   'each thread')
    main(parser.parse_args())
