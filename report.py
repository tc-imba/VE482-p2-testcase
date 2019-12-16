import csv
import os
import tempfile
import shutil
import subprocess
import shlex
import math
import statistics
import re

import click
import jinja2
from test import get_platform, calculate_average_time
import git
import gitfame

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


# https://stackoverflow.com/questions/16259923/how-can-i-escape-latex-special-characters-inside-django-templates
def tex_escape(text):
    """
        :param text: a plain text message
        :return: the message escaped to appear correctly in LaTeX
    """
    conv = {
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '~': r'\textasciitilde{}',
        '^': r'\^{}',
        '\\': r'\textbackslash{}',
        '<': r'\textless{}',
        '>': r'\textgreater{}',
    }
    regex = re.compile('|'.join(re.escape(str(key)) for key in sorted(conv.keys(), key=lambda item: - len(item))))
    return regex.sub(lambda match: conv[match.group()], text)


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
    # print(output)
    return output


def generate_performance_table(data, data_length, base_data):
    output = '\\begin{tabular}{r|%s|cc|cc}\n' % ('c' * data_length)
    output += 'Test Case & \\multicolumn{%d}{c|}{Time (second)} & Average & Base & LogRatio & Score \\\\\\hline' % data_length

    log_ratios = []
    for query, score in PERFORMANCE_QUERY:
        result = data[query]
        log_ratio = 0
        if result['overall_status'] == 'AC':
            log_ratio = max(0.0, math.log2(base_data[query]['average_time'] / result['average_time']))
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
    # print(output)
    return output


def generate_contribution_table(data):
    output = '\\begin{tabular}{r|cc|cc|cc}\n'
    output += 'Author & Lines & \\% & Commits & \\% & Files & \\% \\\\\\hline'
    total_lines = 0
    total_commits = 0
    total_files = 0
    for result in data:
        total_lines += int(result['lines'])
        total_commits += int(result['commits'])
        total_files += int(result['files'])
        output += '\n %s & %s & %s & %s & %s & %s & %s \\\\' % \
                  (tex_escape(result['author']), result['lines'], result['lines%'],
                   result['commits'], result['commits%'], result['files'], result['files%'])
    output += '\\hline'
    output += '\nTotal & %d & 100.0 & %d & 100.0 & %d & 100.0' % (total_lines, total_commits, total_files)
    output += '\n\\end{tabular}\n'
    # print(output)
    return output


def generate_git_log(data):
    output = '\\begin{itemize}\n'
    for commit in data:
        output += '\\item\n'
        output += 'commit %s \\\\\n' % commit['id']
        output += 'Author: %s <%s> \\\\\n' % (tex_escape(commit['author']), tex_escape(commit['email']))
        output += 'Date: %s \\\\\n' % commit['date']
        output += 'Message: %s\n' % tex_escape(commit['message'])

    output += '\\end{itemize}\n'
    # print(output)
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


def get_git_data(project_dir):
    p = subprocess.run(
        ['python', '-m', 'gitfame', '--sort=commits', '-wt', '--incl=.*\\.[cht][ph]{0,2}$', '--format=csv',
         project_dir],
        stdout=subprocess.PIPE,
        universal_newlines=True
    )
    reader = csv.reader(p.stdout.split('\n'))
    first = True
    git_data = {
        'contribution': [],
        'log': [],
    }
    for row in reader:
        if first:
            first = False
            # print(row)
        elif len(row) == 0:
            break
        else:
            git_data['contribution'].append({
                'author': row[0],
                'lines': row[1],
                'lines%': row[4],
                'commits': row[2],
                'commits%': row[5],
                'files': row[3],
                'files%': row[6]
            })

    repo = git.Repo(project_dir)
    for commit in repo.iter_commits('master'):
        git_data['log'].append({
            'id': str(commit),
            'author': str(commit.author),
            'email': str(commit.author.email),
            'date': str(commit.committed_datetime),
            'message': str(commit.message)
        })

    return git_data


@click.command()
@click.option('-p', '--project-dir', required=True, help='LemonDB Directory (a git repo).')
@click.option('-t', '--team', required=True, help='Group Number.')
def main(project_dir, team):
    time_path = os.path.join(project_dir, 'time.csv')
    status_path = os.path.join(project_dir, 'status.csv')

    git_data = get_git_data(project_dir)

    query_data, query_data_length = get_query_data(time_path, status_path)
    base_query_data, base_query_data_length = get_query_data('time.csv', 'status.csv')

    # print(query_data_length)
    # print(query_data)

    with open(os.path.join('report', 'report.tex')) as file:
        template = jinja2.Template(file.read())

    platform_info = get_platform()
    for key in platform_info.keys():
        platform_info[key] = tex_escape(str(platform_info[key]))

    template_data = {
        'team': team,
        'correctness': generate_correctness_table(query_data, query_data_length),
        'performance': generate_performance_table(query_data, query_data_length, base_query_data),
        'contribution': generate_contribution_table(git_data['contribution']),
        'log': generate_git_log(git_data['log']),
        **platform_info
    }

    output_dir = tempfile.mkdtemp(prefix='lemondb.', suffix='.report')
    shutil.copytree('report', output_dir, dirs_exist_ok=True)
    report_path = os.path.abspath(os.path.join(output_dir, 'report.tex'))

    with open(report_path, 'w') as file:
        file.write(template.render(**template_data))

    command = 'xelatex -shell-escape -synctex=1 -interaction=nonstopmode %s' % report_path
    args = shlex.split(command)
    subprocess.run(args, cwd=output_dir, stdout=subprocess.PIPE)
    shutil.copy2(os.path.join(output_dir, 'report.pdf'), os.path.join(project_dir, 'team%s_report.pdf') % team)
    shutil.rmtree(output_dir)


if __name__ == '__main__':
    main()
