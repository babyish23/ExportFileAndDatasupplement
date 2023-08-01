def split_log_file(input_file, output_directory, lines_per_file):
    with open(input_file, 'r', encoding='utf-8') as infile:
        line_count = 0
        file_count = 1
        outfile = None

        for line in infile:
            if line_count % lines_per_file == 0:
                if outfile:
                    outfile.close()
                outfile = open(
                    f"{output_directory}/output_{file_count}.log", 'w', encoding='utf-8')
                file_count += 1

            outfile.write(line)
            line_count += 1

        if outfile:
            outfile.close()


input_file = "output.txt"
output_directory = "./exported_files"
lines_per_file = 100000

split_log_file(input_file, output_directory, lines_per_file)
