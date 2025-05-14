from mrjob.job import MRJob
import csv

class MRParentalEducationPerformance(MRJob):

    def mapper_init(self):
        """Initialize the CSV reader."""
        self.csv_reader = csv.reader(self.stdin, delimiter=';')
        self.header = next(self.csv_reader)  # Skip header row

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line], delimiter=';'))
            fedu = int(row[12])   # Father's education
            medu = int(row[11])   # Mother's education
            avg_grade = (int(row[25]) + int(row[26]) + int(row[27])) / 3
            key = (medu, fedu)
            yield key, avg_grade
        except Exception:
            pass  # Skip malformed lines

    def reducer(self, key, values):
        values = list(values)
        avg = sum(values) / len(values)
        yield key, round(avg, 2)

if __name__ == '__main__':
    MRParentalEducationPerformance.run()
