import os


def generate(query, listen_query, dest, levels):
    files = 2 ** levels
    with open(query) as f:
        lines = f.readlines()
    lines_per_file = len(lines) // files
    for i in range(files * 2 - 1):
        if i == 0:
            filename = os.path.join(dest, listen_query)
        else:
            filename = os.path.join(dest, listen_query) + ".part" + str(i)
        with open(filename, 'w') as f:
            if i < files - 1:
                f.write('LISTEN ( %s.part%d );\n' % (listen_query, (i + 1) * 2 - 1))
                f.write('LISTEN ( %s.part%d );\n' % (listen_query, (i + 1) * 2))
            else:
                j = i - files + 1
                if j == files - 1:
                    f.writelines(lines[j * lines_per_file:])
                else:
                    f.writelines(lines[j * lines_per_file: (j + 1) * lines_per_file])


def generate2(query, listen_query, dest, split_list):
    def get_count(_list):
        p_count = 0
        f_count = 1
        for element in _list:
            if isinstance(element, list):
                a, b = get_count(element)
                p_count += a
                f_count += b
            else:
                p_count += 1
        return p_count, f_count

    paragraphs, files = get_count(split_list)
    print(paragraphs, files)

    with open(query) as f:
        lines = f.readlines()
    lines_per_paragraph = len(lines) // paragraphs

    def generate_file(_list, p_i=0, f_i=0):
        print(p_i, f_i)
        if f_i == 0:
            filename = os.path.join(dest, listen_query)
        else:
            filename = os.path.join(dest, listen_query) + ".part" + str(f_i)
        with open(filename, 'w') as f:
            for element in _list:
                if isinstance(element, list):
                    f.write('LISTEN ( %s.part%d );\n' % (listen_query, f_i + 1))
                    p_i, f_i = generate_file(element, p_i, f_i + 1)
                else:
                    if p_i == paragraphs - 1:
                        f.writelines(lines[p_i * lines_per_paragraph:])
                    else:
                        f.writelines(lines[p_i * lines_per_paragraph: (p_i + 1) * lines_per_paragraph])
                    p_i += 1
        return p_i, f_i

    generate_file(split_list)


if __name__ == '__main__':
    generate2('query/test.query', 'test_listen_1.query', 'query', [[1]])
    generate2('query/test.query', 'test_listen_2.query', 'query', [[1], 1])
    generate2('query/test.query', 'test_listen_3.query', 'query', [1, [1], 1])
    generate2('query/test.query', 'test_listen_4.query', 'query', [[1], 1, [1], 1, [1]])
    generate2('query/test.query', 'test_listen_5.query', 'query', [[[1], [1]]])
    generate2('query/test.query', 'test_listen_6.query', 'query', [[[1], [1]], 1, [[1], [1]]])
    generate2('query/test.query', 'test_listen_7.query', 'query', [[[[1], [1]], [[1], [1]]], [[[1], [1]], [[1], [1]]]])
    generate2('query/test.query', 'test_listen_8.query', 'query', [1, [1, [1, [1, [1, [1, [1, [1]]]]]]]])

    # generate('query/test.query', 'listen_test.query', 'query', 1)
    # generate('query/single_read.query', 'listen_read.query', 'query', 3)
    # generate('query/single_read_dup.query', 'listen_read_dup.query', 'query', 3)
    # generate('query/single_read_update.query', 'listen_read_update.query', 'query', 3)
    # generate('query/single_insert_delete.query', 'listen_insert_delete.query', 'query', 3)
