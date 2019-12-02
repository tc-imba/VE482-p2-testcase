"""
LemonDB Test
by tc-imba <liuyh615@sjtu.edu.cn>
"""

import csv
import os
import shutil
import subprocess
import time
import filecmp
# import logger

import click
import cpuinfo
import humanize
import psutil
import memory_tempfile
import progressbar
from logzero import logger

TEST_QUERY = [
    ('test', 1),
    ('single_read', 25),
    ('single_read_update', 25),
    ('single_read_dup', 35),
    ('single_insert_delete', 20),
    # 'few_read',
    # 'few_read_update',
    # 'few_read_dup',
    # 'few_insert_delete',
    # 'many_read',
    # 'many_read_update',
    # 'many_read_dup',
    # 'many_insert_delete',
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


def build(project_dir, build_dir, clean=False):
    working_dir = os.getcwd()
    logger.info("Build program for %s", project_dir)

    os.chdir(project_dir)
    if clean:
        shutil.rmtree(build_dir, ignore_errors=True)
    os.makedirs(build_dir, exist_ok=True)
    os.chdir(build_dir)

    if execute("cmake", "-DCMAKE_BUILD_TYPE=Release", "..") != 0:
        logger.error("CMake failed!")
        exit(-1)

    if execute("make", "-j4") != 0:
        logger.error("Make failed!")
        exit(-1)

    if not os.path.exists("lemondb"):
        logger.error("Program not found!")
        exit(-1)
    logger.info("Build program for %s succeeded!", project_dir)
    os.chdir(working_dir)
    return os.path.join(project_dir, build_dir, 'lemondb')


def run(program, data, temp_dir, timeout=1000, answer_dir=None):
    if answer_dir:
        answer_dir = os.path.abspath(answer_dir)

    working_dir = os.getcwd()
    runtime_dir = os.path.join(temp_dir, 'runtime')
    shutil.rmtree(runtime_dir, ignore_errors=True)
    os.makedirs(runtime_dir, exist_ok=True)
    os.chdir(runtime_dir)

    os.mkfifo('test.fifo')
    out_fd = os.open('stdout', os.O_WRONLY | os.O_CREAT)
    status = None

    p = subprocess.Popen([program, "--listen=test.fifo"], stdout=out_fd, stderr=subprocess.DEVNULL)
    start = time.time_ns()

    try:
        fd = os.open('test.fifo', os.O_WRONLY)
        os.write(fd, data)
        os.close(fd)
        p.wait(timeout)
    except subprocess.TimeoutExpired:
        status = "TLE"
    except:
        status = "RTE"

    end = time.time_ns()

    os.close(out_fd)
    os.remove('test.fifo')

    realtime = (end - start) / 1e9
    os.chdir(working_dir)

    if answer_dir:
        dcmp = filecmp.dircmp(answer_dir, runtime_dir)
        if dcmp.diff_files:
            status = "WA"

    return status, realtime


def test(program, query, data_dir, temp_dir, times=5, generate_answer=False, suggest_timeout=0):
    working_dir = os.getcwd()
    db_dir = os.path.join(temp_dir, 'db')
    query_dir = os.path.join(data_dir, 'query')
    answer_dir = os.path.join(data_dir, 'answer', query)

    program = os.path.abspath(program)
    query_file = os.path.join(query_dir, query + '.query')
    results = []

    with open(query_file, 'rb') as f:
        data = f.read()

    if generate_answer:
        logger.info('Generate answer for %s.query ...', query)
        for i in range(times):
            status, realtime = run(program, data, temp_dir)
            update_pbar(suggest_timeout)
            results.append((status, realtime))
            logger.info('%2d: %.3f s', i + 1, realtime)

        os.makedirs(answer_dir, exist_ok=True)
        shutil.rmtree(answer_dir, ignore_errors=True)
        runtime_dir = os.path.join(temp_dir, 'runtime')
        shutil.copytree(runtime_dir, answer_dir)
    else:
        logger.info('Test %s.query ...', query)
        if not os.path.exists(answer_dir):
            logger.error('Error: answer not found!')
            exit(-1)
        for i in range(times):
            status, realtime = run(program, data, temp_dir, timeout=suggest_timeout * 2, answer_dir=answer_dir)
            update_pbar(suggest_timeout)
            results.append((status, realtime))
            logger.info('%2d: %.3f s', i + 1, realtime)

    os.chdir(working_dir)
    return results


def get_platform():
    cpu_info = cpuinfo.get_cpu_info()
    memory_info = psutil.virtual_memory()
    return {
        'cpu': cpu_info['brand'],
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


def update_pbar(value):
    global pbar
    global progress_now
    if pbar:
        progress_now += value
        pbar.update(progress_now)


@click.command()
@click.option('-p', '--project-dir', required=True, help='LemonDB Directory.')
@click.option('--rebuild', is_flag=True, help='Rebuild tmpfs and project')
@click.option('-d', '--data-dir', default='.', help='Data Directory (contains sample and db).')
@click.option('--generate-answer', is_flag=True, help='Generate answer.')
@click.option('--times', default=5, type=int)
def main(project_dir, rebuild, data_dir, generate_answer, times):
    global pbar
    progressbar.streams.wrap_stderr()

    platform_info = get_platform()
    logger.info(platform_info)

    temp_dir = init_tmpfs(data_dir)
    program = build(project_dir, 'build', rebuild)
    answer_time_path = os.path.join(data_dir, 'answer', 'time.csv')
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
        result = test(program, query, data_dir, temp_dir,
                      generate_answer=generate_answer, times=times,
                      suggest_timeout=base_time[query])
        results.append(result)

        # break
    pbar.finish()

    logger.debug(results)
    if generate_answer:
        with open(answer_time_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['query'] + list(map(lambda x: str(x + 1), range(times))))
            for i in range(len(TEST_QUERY)):
                writer.writerow([TEST_QUERY[i][0]] + list(map(lambda x: str(x[1]), results[i])))

    # print   (program)


if __name__ == '__main__':
    main()
