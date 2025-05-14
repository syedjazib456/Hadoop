from mrjob.job import MRJob
import csv

class MRParentalEducationPerformance(MRJob):

    def mapper(self, _, line):
        # Convert bytes to str if needed
        if isinstance(line, bytes):
            line = line.decode('utf-8')

        if line.startswith("school;sex;age"):
            return  # skip header

        try:
            row = next(csv.reader([line], delimiter=';'))

            # Ensure row has enough columns
            if len(row) < 28:
                return

            fedu = int(row[12])   # Father's education
            medu = int(row[11])   # Mother's education
            avg_grade = (int(row[25]) + int(row[26]) + int(row[27])) / 3
            key = (medu, fedu)
            yield key, avg_grade
        except Exception as e:
            # For debugging only: write to stderr
            import sys
            print(f"Error parsing line: {line}\nException: {e}", file=sys.stderr)

    def reducer(self, key, values):
        values = list(values)
        avg = sum(values) / len(values)
        yield key, round(avg, 2)

if __name__ == '__main__':
    MRParentalEducationPerformance.run()
