from mrjob.job import MRJob
import csv

class MRParentalEducationPerformance(MRJob):

    def mapper(self, _, line):
        # Convert bytes to str if needed
        if isinstance(line, bytes):
            line = line.decode('utf-8')

        # Skip header line
        if line.startswith("school,sex,age,address,famsize,Pstatus,Medu,Fedu,Mjob,Fjob,reason,guardian,traveltime,studytime,failures,schoolsup,famsup,paid,activities,nursery,higher,internet,romantic,famrel,freetime,goout,Dalc,Walc,health,absences,G1,G2,G3"):
            return  # skip header

        try:
            row = next(csv.reader([line], delimiter=','))  # Default CSV delimiter is comma

            # Ensure row has enough columns
            if len(row) < 33:
                return

            fedu = int(row[7])   # Father's education (adjusted column index)
            medu = int(row[6])   # Mother's education (adjusted column index)
            avg_grade = (int(row[30]) + int(row[31]) + int(row[32])) / 3  # G1, G2, G3 grades
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
