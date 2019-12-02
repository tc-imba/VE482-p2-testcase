"""
LemonDB Test
bt tc-imba <liuyh615@sjtu.edu.cn>
"""

import os
import shutil
import subprocess
import platform
import time

import click


def build(project_dir, build_dir, clean=False):
    working_dir = os.getcwd()
    print("Build program for %s" % project_dir)
    os.chdir(project_dir)
    if clean:
        shutil.rmtree(build_dir, ignore_errors=True)
    os.makedirs(build_dir, exist_ok=True)
    os.chdir(build_dir)
    p = subprocess.run(["cmake", "-DCMAKE_BUILD_TYPE=Debug", ".."])
    # p = subprocess.run(["cmake", "-DCMAKE_BUILD_TYPE=Release", ".."])
    if p.returncode != 0:
        print("CMake failed!")
        exit(-1)
    p = subprocess.run(["make", "-j4"])
    if p.returncode != 0:
        print("Make failed!")
        exit(-1)
    if not os.path.exists("lemondb"):
        print("Program not found!")
        exit(-1)
    print("Build program for %s succeeded!\n" % project_dir)
    os.chdir(working_dir)
    return os.path.join(project_dir, build_dir, 'lemondb')


def test(program, query_file, data_dir):
    working_dir = os.getcwd()
    program = os.path.abspath(program)

    with open(query_file, 'rb') as f:
        data = f.read()

    db_dir = os.path.join(data_dir, 'db')
    os.chdir(data_dir)
    shutil.rmtree('temp', ignore_errors=True)
    os.makedirs('temp', exist_ok=True)
    os.chdir('temp')

    if os.path.exists('test.fifo'):
        os.remove('test.fifo')
    os.mkfifo('test.fifo')

    p = subprocess.Popen([program, "--listen=test.fifo"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    fd = os.open('test.fifo', os.O_WRONLY)
    os.write(fd, data)

    p.wait(100)

    os.chdir(working_dir)


@click.command()
@click.option('-p', '--project-dir', required=True, help='LemonDB Directory.')
@click.option('--rebuild', default=False)
@click.option('-d', '--data-dir', required=True, help='Data Directory (contains sample and db).')
def main(project_dir, rebuild, data_dir):
    print(platform.machine())


    # print(project_dir)
    program = build(project_dir, 'build', rebuild)
    query_dir = os.path.join(data_dir, 'sample')
    for query_file in sorted(os.listdir(query_dir)):
        # test(program, os.path.join(query_dir, query_file), data_dir)
        break
    # print(program)


if __name__ == '__main__':
    main()
