"""
LemonDB Test
by tc-imba <liuyh615@sjtu.edu.cn>
"""

import csv
import os
import shutil
import subprocess
import time
import multiprocessing
import threading
from queue import Queue
import re
import platform

import click
import cpuinfo
import humanize
import psutil
import memory_tempfile
import progressbar
from logzero import logger

TEST_QUERY = [
    ('test_quit', 0),
    ('test_management', 1),
    ('test_all', 1),
    ('test_listen_1', 1),
    ('test_listen_2', 1),
    ('test_listen_3', 1),
    ('test_listen_4', 1),
    ('test_listen_5', 1),
    ('test_listen_6', 1),
    ('test_listen_7', 1),
    ('test_listen_8', 1),
    ('single_read', 25),
    ('single_read_update', 25),
    ('single_read_dup', 35),
    ('single_insert_delete', 20),
    ('few_read', 20),
    ('few_read_update', 25),
    ('few_read_dup', 105),
    ('few_insert_delete', 15),
    ('many_read', 20),
    ('many_read_update', 20),
    ('many_read_dup', 20),
    ('many_insert_delete', 20),
]


def execute(*args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while p.poll() is None:
        flag = True
        while True:
            out = p.stdout.readline()
            if out:
                logger.info(out.decode('utf-8').strip('\n'))
                flag = False
            else:
                break
        while True:
            err = p.stderr.readline()
            if err:
                logger.warning(err.decode('utf-8').strip('\n'))
                flag = False
            else:
                break
        if flag:
            time.sleep(0.1)
    return p.returncode


def build(project_dir, build_dir, threads, clean=False):
    working_dir = os.getcwd()
    logger.info("Build program for %s", project_dir)

    os.chdir(project_dir)
    if clean:
        shutil.rmtree(build_dir, ignore_errors=True)
    os.makedirs(build_dir, exist_ok=True)
    os.chdir(build_dir)

    # if execute("cmake", "-DCMAKE_BUILD_TYPE=Debug", "..") != 0:
    if execute("cmake", "-DCMAKE_BUILD_TYPE=Release", "..") != 0:
        logger.error("CMake failed!")
        exit(-1)

    if execute("make", "-j" + str(threads)) != 0:
        logger.error("Make failed!")
        exit(-1)

    if not os.path.exists("lemondb"):
        logger.error("Program not found!")
        exit(-1)
    logger.info("Build program for %s succeeded!", project_dir)
    os.chdir(working_dir)
    return os.path.join(project_dir, build_dir, 'lemondb')


# simply sort tables by lines because they are unordered
def sort_tables(table_dir):
    for filename in os.listdir(table_dir):
        if filename.endswith('.tbl'):
            filename = os.path.join(table_dir, filename)
            subprocess.run(['sort', filename, '-o', filename])


def __run(q, program, base_query_file, query_files, temp_dir, threads, answer_dir):
    if answer_dir:
        answer_dir = os.path.abspath(answer_dir)

    working_dir = os.getcwd()
    runtime_dir = os.path.join(temp_dir, 'runtime')

    status = "AC"
    start = 0
    realtime = 0,
    exception = None
    p = None

    def continue_pipe_query_file(query_file_stack, line_now, close_current=False):
        if len(query_file_stack) == 0:
            return line_now
        query_file_now, fd, i = query_file_stack[-1]
        # print(query_file_now, fd, i, line_now)
        if i == len(query_files[query_file_now]):
            # print("close " + query_file_now)
            os.close(fd)
            query_file_stack.pop()
            return continue_pipe_query_file(query_file_stack, line_now, close_current)
        if close_current:
            return line_now
        data_lines, data = query_files[query_file_now][i]
        query_file_stack[-1] = (query_file_now, fd, i + 1)
        if data_lines > 0:
            # print(data)
            os.write(fd, data)
            line_now += data_lines
            if i + 1 != len(query_files[query_file_now]):
                return continue_pipe_query_file(query_file_stack, line_now)
            return continue_pipe_query_file(query_file_stack, line_now, True)
        else:
            # print("open " + data)
            fd = os.open(data, os.O_WRONLY)
            query_file_stack.append((data, fd, 0))
            return continue_pipe_query_file(query_file_stack, line_now)

    def __continue_pipe_query_file(q, query_file_stack, line_now):
        result = continue_pipe_query_file(query_file_stack, line_now)
        q.put((result, query_file_stack))

    def try_continue_pipe_query_file(thread, queue, query_file_stack, line, line_expect):
        if thread:
            thread.join(0)
            if not thread.is_alive():
                line_expect, query_file_stack = queue.get()
                line_expect = min(line_max, line_expect)
                thread = None
        if thread is None:
            if line.isdigit() and int(line) >= line_expect and (int(line) < line_max or line_max == 0):
                # print('finish:', line_expect)
                thread = threading.Thread(target=__continue_pipe_query_file, daemon=True,
                                          args=(queue, query_file_stack, line_expect))
                thread.start()
        return thread, query_file_stack, line_expect

    try:
        # mutex = multiprocessing.Lock()

        shutil.rmtree(runtime_dir, ignore_errors=True)
        os.makedirs(runtime_dir, exist_ok=True)
        os.chdir(runtime_dir)

        line_max = -1
        for query_file, query_file_data in query_files.items():
            os.mkfifo(query_file)
            for data_lines, data in query_file_data:
                line_max += data_lines

        # out_fd = os.open('stdout', os.O_WRONLY | os.O_CREAT)

        p = subprocess.Popen([program, "--listen=" + base_query_file, "--threads=" + str(threads)],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL,
                             universal_newlines=True
                             )
        start = time.time_ns()

        base_query_fd = os.open(base_query_file, os.O_WRONLY)
        query_file_stack = [(base_query_file, base_query_fd, 0)]

        queue = Queue()
        thread, query_file_stack, line_expect = try_continue_pipe_query_file(None, queue, query_file_stack, "0", 0)

        stdout = []
        while p.poll() is None:
            line = p.stdout.readline()
            stdout.append(line)
            line = line.rstrip()
            # print(line)
            thread, query_file_stack, line_expect = try_continue_pipe_query_file(
                thread, queue, query_file_stack, line, line_expect)

        end = time.time_ns()

        if p.returncode != 0:
            status = "RTE"

        with open('stdout', 'w') as f:
            f.writelines(stdout)

        # os.close(out_fd)
        for query_file in query_files.keys():
            os.remove(query_file)

        realtime = (end - start) / 1e9
        os.chdir(working_dir)

        sort_tables(runtime_dir)
        if answer_dir and status == "AC":
            diff = subprocess.run(["diff", "-qbB", answer_dir, runtime_dir],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if diff.returncode != 0:
                status = "WA"

    except subprocess.TimeoutExpired:
        status = "TLE"
    except Exception as e:
        status = "RTE"
        exception = e

    if start != 0 and realtime == 0:
        end = time.time_ns()
        realtime = (end - start) / 1e9

    try:
        if p and p.poll() is None:
            p.kill()
    except Exception as e:
        status = "RTE"
        exception = e

    if not isinstance(realtime, (int, float)):
        realtime = 0
    q.put((status, realtime, exception))


def run(program, base_query_file, query_files, temp_dir, threads, timeout=1000.0, answer_dir=None):
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=__run,
                                args=(q, program, base_query_file, query_files, temp_dir, threads, answer_dir,))
    p.start()
    p.join(timeout)
    p.kill()
    # kill lemondb with pkill
    subprocess.run(['pkill', '-f', 'lemondb'])
    if p.exitcode == 0:
        status, realtime, exception = q.get()
        if exception:
            logger.exception(exception)
        return status, realtime
    else:
        return "TLE", timeout


def read_query(query_dir, query):
    query_files = {}

    def add_query_file(filename):
        if filename in query_files:
            return
        query_files[filename] = []
        with open(os.path.join(query_dir, filename), 'r') as f:
            lines = f.readlines()
        start = 0
        for i, line in enumerate(lines):
            if line.startswith("LISTEN"):
                query_files[filename].append((i + 1 - start, ''.join(lines[start:i + 1]).encode('UTF-8')))
                result = re.findall(r"\(.+\)", line)
                if result:
                    listen_query_filename = result[0].strip('( )')
                    add_query_file(listen_query_filename)
                    query_files[filename].append((0, listen_query_filename))
                start = i + 1
        if start < len(lines):
            query_files[filename].append((len(lines) - start, ''.join(lines[start:]).encode('utf-8')))

    add_query_file(query)
    return query_files


def test(program, query, data_dir, temp_dir, threads, times=5, generate_answer=False, suggest_timeout=0):
    working_dir = os.getcwd()
    db_dir = os.path.join(temp_dir, 'db')
    query_dir = os.path.join(data_dir, 'query')
    answer_dir = os.path.join(data_dir, 'answer', query)

    base_query_file = query + '.query'
    query_files = read_query(query_dir, base_query_file)
    # import pprint
    # pprint.pprint(query_files)
    results = []

    # exit(1)
    #
    # with open(query_file, 'rb') as f:
    #     data = f.read()

    if generate_answer:
        logger.info('Generate answer for %s.query ...', query)
        for i in range(times):
            status, realtime = run(program, base_query_file, query_files, temp_dir, threads)
            update_pbar(suggest_timeout)
            results.append((status, realtime))
            logger.info('%2d: %s %.3f s', i + 1, status, realtime)

        if results[-1][0] == "AC":
            os.makedirs(answer_dir, exist_ok=True)
            shutil.rmtree(answer_dir, ignore_errors=True)
            runtime_dir = os.path.join(temp_dir, 'runtime')
            shutil.copytree(runtime_dir, answer_dir)
        else:
            logger.error('Error: %s', results[-1][0])

    else:
        logger.info('Test %s.query ...', query)
        if not os.path.exists(answer_dir):
            logger.error('Error: answer not found!')
            exit(-1)
        for i in range(times):
            status, realtime = run(program, base_query_file, query_files, temp_dir, threads,
                                   timeout=max(5.0, suggest_timeout * 1.2), answer_dir=answer_dir)
            results.append((status, realtime))
            logger.info('%2d: %s %.3f s', i + 1, status, realtime)
            if status == "AC":
                update_pbar(suggest_timeout)
            else:
                update_pbar(suggest_timeout * (times - i))
                break

    os.chdir(working_dir)
    return results


def get_platform():
    cpu_info = cpuinfo.get_cpu_info()
    memory_info = psutil.virtual_memory()
    brand = ''
    if 'brand_raw' in cpu_info:
        brand = cpu_info['brand_raw']
    elif 'brand' in cpu_info:
        brand = cpu_info['brand']
    return {
        'platform': platform.platform(),
        'cpu': brand,
        'threads': cpu_info['count'],
        'memory': humanize.naturalsize(memory_info.total, gnu=True)
    }


def init_tmpfs(data_dir, clean=False):
    tempfile = memory_tempfile.MemoryTempfile(preferred_paths=['/run/user/{uid}'],
                                              remove_paths=['/dev/shm', '/run/shm'],
                                              additional_paths=['/var/run'], filesystem_types=['tmpfs'], fallback=True)
    temp_dir = tempfile.gettempdir()
    if temp_dir:
        logger.info('Using memory-based tmpfs: %s', temp_dir)
    else:
        logger.error('Error: tmpfs not found!')
        exit(-1)

    temp_dir = os.path.join(temp_dir, 'lemondb')
    if clean:
        shutil.rmtree(temp_dir, ignore_errors=True)
    os.makedirs(temp_dir, exist_ok=True)

    temp_db_dir = os.path.join(temp_dir, 'db')
    db_dir = os.path.join(data_dir, 'db')
    if not os.path.exists(temp_db_dir):
        shutil.copytree(db_dir, temp_db_dir)

    logger.info('DB initialized on %s', temp_dir)
    return os.path.abspath(temp_dir)


def calculate_average_time(data):
    data = sorted(data)
    if len(data) == 0:
        return 0
    if len(data) > 2:
        data = data[1:len(data) - 1]
    return sum(data) / len(data)


pbar = None
progress_now = 0
progress_max_value = 0


def update_pbar(value):
    global pbar
    global progress_now
    if pbar:
        progress_now += value
        pbar.update(min(progress_max_value, progress_now))


def save_result(results, columns, time_path, status_path):
    with open(time_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['query'] + list(map(lambda x: str(x + 1), range(columns))))
        for i in range(len(TEST_QUERY)):
            writer.writerow([TEST_QUERY[i][0]] + list(map(lambda x: str(x[1]), results[i])))
    with open(status_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['query'] + list(map(lambda x: str(x + 1), range(columns))))
        for i in range(len(TEST_QUERY)):
            writer.writerow([TEST_QUERY[i][0]] + list(map(lambda x: str(x[0]), results[i])))


@click.command()
@click.option('-p', '--project-dir', help='LemonDB Directory.')
@click.option('-b', '--binary', default='', help='LemonDB Binary.')
@click.option('--rebuild', is_flag=True, help='Rebuild tmpfs and project')
@click.option('-d', '--data-dir', default='.', help='Data Directory (contains sample and db).')
@click.option('--generate-answer', is_flag=True, help='Generate answer.')
@click.option('--times', default=5, type=int)
@click.option('--threads', default=0, type=int)
def main(project_dir, binary, rebuild, data_dir, generate_answer, times, threads):
    global pbar, progress_max_value
    progressbar.streams.wrap_stderr()

    platform_info = get_platform()
    if threads == 0:
        threads = int(platform_info['threads'])
    logger.info(platform_info)

    temp_dir = init_tmpfs(data_dir)
    if project_dir:
        project_dir = os.path.abspath(project_dir)
        program = build(project_dir, 'build', threads, clean=rebuild)
    else:
        if binary:
            program = binary
        else:
            program = os.path.join(os.path.dirname(__file__), 'bin', 'lemondb')
        project_dir = os.path.abspath(os.path.dirname(program))
    program = os.path.abspath(program)

    logger.info('Project Dir: %s', project_dir)
    logger.info('LemonDB: %s', program)

    answer_time_path = os.path.join(data_dir, 'answer', 'time.csv')
    answer_status_path = os.path.join(data_dir, 'answer', 'status.csv')
    results = []
    base_time = {}
    total_base_time = 0

    if not generate_answer:
        if not os.path.exists(answer_time_path):
            logger.error('Error: answer not found!')
            exit(-1)
        with open(answer_time_path) as f:
            reader = csv.reader(f)
            first_row = True
            for row in reader:
                if first_row:
                    first_row = False
                else:
                    query = row[0]
                    time_data = list(map(lambda x: float(x), row[1:]))
                    base_time[query] = calculate_average_time(time_data)
                    total_base_time += base_time[query]
    else:
        for query, unit_time in TEST_QUERY:
            base_time[query] = unit_time
            total_base_time += base_time[query]

            # widgets = [progressbar.Bar('>'), ' ', progressbar.ETA()]
    # pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(TEST_QUERY) * times).start()
    widgets = [
        ' [', progressbar.Percentage(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') \n']
    progress_max_value = total_base_time * times
    pbar = progressbar.ProgressBar(max_value=progress_max_value, widgets=widgets).start()

    for query, unit_time in TEST_QUERY:
        result = test(program, query, data_dir, temp_dir, threads,
                      generate_answer=generate_answer, times=times,
                      suggest_timeout=base_time[query])
        results.append(result)

        # break
    pbar.finish()

    logger.debug(results)
    if generate_answer:
        save_result(results, times, answer_time_path, answer_status_path)
    else:
        time_path = os.path.join(project_dir, 'time.csv')
        status_path = os.path.join(project_dir, 'status.csv')
        save_result(results, times, time_path, status_path)


if __name__ == '__main__':
    main()
