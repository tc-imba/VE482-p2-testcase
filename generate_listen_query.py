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


if __name__ == '__main__':
    generate('query/test.query', 'listen_test.query', 'query', 1)
    generate('query/single_read.query', 'listen_read.query', 'query', 3)
    generate('query/single_read_dup.query', 'listen_read_dup.query', 'query', 3)
    generate('query/single_read_update.query', 'listen_read_update.query', 'query', 3)
    generate('query/single_insert_delete.query', 'listen_read_delete.query', 'query', 3)
