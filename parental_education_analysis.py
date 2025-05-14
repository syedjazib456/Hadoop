from mrjob.job import MRJob
import csv

class MRParentalEducationPerformance(MRJob):

    def mapper(self, _, line):
        # Skip header manually using a simple check
        if line.startswith("school;sex;age"):
            return  # skip header line

        try:
            row = next(csv.reader([line], delimiter=';'))
            fedu = int(row[12])   # Father's education
            medu = int(row[11])   # Mother's education
            avg_grade = (int(row[25]) + int(row[26]) + int(row[27])) / 3
            key = (medu, fedu)
            yield key, avg_grade
        except Exception as e:
            print("Error parsing line:", line)
            print("Exception:", e)

    def reducer(self, key, values):
        values = list(values)
        avg = sum(values) / len(values)
        yield key, round(avg, 2)

if __name__ == '__main__':
    MRParentalEducationPerformance.run()
