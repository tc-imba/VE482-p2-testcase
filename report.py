import csv
import os
import tempfile
import shutil
import subprocess
import shlex
import math
import statistics

import click
import jinja2
from test import get_platform, calculate_average_time

CORRECTNESS_QUERY = [
    ('test_quit', 2),
    ('test_management', 3),
    ('test_all', 15),
]
LISTEN_QUERY = [
    ('test_listen_1', 1),
    ('test_listen_2', 1),
    ('test_listen_3', 1),
    ('test_listen_4', 1),
    ('test_listen_5', 1),
    ('test_listen_6', 1),
    ('test_listen_7', 1),
    ('test_listen_8', 1),
]
LISTEN_SCORE = [
    (1, 2),
    (2, 3),
    (4, 3),
    (8, 2),
]
PERFORMANCE_QUERY = [
    ('single_read', 1),
    ('single_read_update', 1),
    ('single_read_dup', 1),
    ('single_insert_delete', 1),
    ('few_read', 1),
    ('few_read_update', 1),
    ('few_read_dup', 1),
    ('few_insert_delete', 1),
    ('many_read', 1),
    ('many_read_update', 1),
    ('many_read_dup', 1),
    ('many_insert_delete', 1),
]


def generate_result_times(data, data_length, remove_min_max=False):
    output = ''
    max_value = -1
    min_value = 1e9
    max_index = 0
    min_index = 0
    for i in range(len(data['time_data'])):
        if data['time_data'][i] > max_value:
            max_value = data['time_data'][i]
            max_index = i
        if data['time_data'][i] < min_value:
            min_value = data['time_data'][i]
            min_index = i

    for i in range(data_length):
        if len(data['time_data']) > i:
            if data['status_data'][i] == 'AC':
                if remove_min_max and data['overall_status'] == 'AC' and (i == max_index or i == min_index):
                    output += '\\sout{%.3f}' % data['time_data'][i]
                else:
                    output += '%.3f' % data['time_data'][i]
            else:
                output += '{\\color{red}%s}' % data['status_data'][i]
        else:
            output += '/'
        if i + 1 < data_length:
            output += ' & '
    return output


def generate_correctness_table(data, data_length):
    output = '\\begin{tabular}{r|%s|cc}\n' % ('c' * data_length)
    output += 'Test Case & \\multicolumn{%d}{c|}{Time (second)} & Score & Out of  \\\\\\hline' % data_length
    total_score = 0
    for query, score in CORRECTNESS_QUERY:
        result = data[query]
        result_times = generate_result_times(result, data_length)
        result_score = result['overall_status'] == 'AC' and score or 0
        total_score += result_score
        output += '\n%s & %s & %s & %s \\\\' % (query[5:], result_times, result_score, score)
    output += '\\hline'
    listen_ac = 0
    for query, score in LISTEN_QUERY:
        if data[query]['overall_status'] == 'AC':
            listen_ac += 1
    listen_score = 0
    for num, score in LISTEN_SCORE:
        if listen_ac >= num:
            listen_score += score
    total_score += listen_score
    first = True
    for query, score in LISTEN_QUERY:
        result = data[query]
        result_times = generate_result_times(result, data_length)
        ending = ''
        if first:
            first = False
            ending += '\\multirow{%d}{*}{%d} & ' % (len(LISTEN_QUERY), listen_score)
            ending += '\\multirow{%d}{*}{%d}' % (len(LISTEN_QUERY), 10)
        output += '\n%s & %s & %s \\\\' % (query[5:].replace('_', ' '), result_times, ending)
    output += '\\hline'
    output += '\nTotal & \\multicolumn{%d}{c|}{/} & %d & %d' % (data_length, total_score, 30)
    output += '\n\\end{tabular}\n'
    print(output)
    return output


def generate_performance_table(data, data_length, base_data):
    output = '\\begin{tabular}{r|%s|cc|cc}\n' % ('c' * data_length)
    output += 'Test Case & \\multicolumn{%d}{c|}{Time (second)} & Average & Base & LogRatio & Score \\\\\\hline' % data_length

    log_ratios = []
    for query, score in PERFORMANCE_QUERY:
        result = data[query]
        log_ratio = 0
        if result['overall_status'] == 'AC':
            log_ratio = math.log2(max(0.0, base_data[query]['average_time'] / result['average_time']))
        log_ratios.append(log_ratio)
    log_ratio_stddev = statistics.stdev(log_ratios)
    log_ratio_sum = sum(log_ratios)
    total_score = 60 * max(0.0, log_ratio_sum / len(log_ratios)) / (0.6 + 0.2 * log_ratio_stddev)

    for i, (query, score) in enumerate(PERFORMANCE_QUERY):
        result = data[query]
        result_times = generate_result_times(result, data_length, True)
        base_time = base_data[query]['average_time']
        average_time = '/'
        log_ratio = log_ratios[i]
        score = total_score * log_ratio / log_ratio_sum
        if result['overall_status'] == 'AC':
            average_time = '%.3f' % result['average_time']
        output += '\n%s & %s & %s & %.3f & %.3f & %.3f \\\\' % (
            query.replace('_', ' '), result_times, average_time, base_time, log_ratio, score)

    output += '\\hline'
    output += '\nTotal & \\multicolumn{%d}{c|}{/} & \\multicolumn{2}{c|}{/} & %.3f & %.3f' % (
        data_length, log_ratio_sum / len(log_ratios), total_score)
    output += '\n\\end{tabular}\n'
    print(output)
    return output


def get_query_data(time_path, status_path):
    query_data = {}
    query_data_length = 0
    with open(time_path) as t_file, open(status_path) as status_file:
        reader = csv.reader(t_file)
        first_row = True
        for row in reader:
            if first_row:
                first_row = False
            else:
                query = row[0]
                time_data = list(map(lambda x: float(x), row[1:]))
                query_data_length = max(query_data_length, len(time_data))
                query_data[query] = {
                    'time_data': time_data,
                    'average_time': calculate_average_time(time_data)
                }
        reader = csv.reader(status_file)
        first_row = True
        for row in reader:
            if first_row:
                first_row = False
            else:
                query = row[0]
                status_data = row[1:]
                overall_status = "AC"
                for status in status_data:
                    if status != "AC":
                        overall_status = "ERR"
                        break
                query_data[query]['status_data'] = status_data
                query_data[query]['overall_status'] = overall_status
                query_data_length = max(query_data_length, len(status_data))
    return query_data, query_data_length


@click.command()
@click.option('-p', '--project-dir', required=True, help='LemonDB Directory.')
@click.option('-g', '--group', required=True, help='Group Number.')
def main(project_dir, group):
    time_path = os.path.join(project_dir, 'time.csv')
    status_path = os.path.join(project_dir, 'status.csv')

    query_data, query_data_length = get_query_data(time_path, status_path)
    base_query_data, base_query_data_length = get_query_data('time.csv', 'status.csv')

    # print(query_data_length)
    # print(query_data)

    with open(os.path.join('report', 'report.tex')) as file:
        template = jinja2.Template(file.read())

    template_data = {
        'group': group,
        'correctness': generate_correctness_table(query_data, query_data_length),
        'performance': generate_performance_table(query_data, query_data_length, base_query_data),
        **get_platform()
    }

    output_dir = tempfile.mkdtemp(prefix='lemondb.', suffix='.report')
    shutil.copytree('report', output_dir, dirs_exist_ok=True)
    report_path = os.path.abspath(os.path.join(output_dir, 'report.tex'))

    with open(report_path, 'w') as file:
        file.write(template.render(**template_data))

    command = 'xelatex -shell-escape -synctex=1 -interaction=nonstopmode %s' % report_path
    args = shlex.split(command)
    subprocess.run(args, cwd=output_dir, stdout=subprocess.PIPE)
    shutil.copy2(os.path.join(output_dir, 'report.pdf'), os.path.join(project_dir, 'report.pdf'))
    shutil.rmtree(output_dir)


if __name__ == '__main__':
    main()
