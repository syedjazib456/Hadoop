from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class StudentPerformanceAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init_gender_grades,  # Use separate mapper_init
                   mapper=self.map_gender_grades,
                   reducer=self.reduce_gender_grades),
            MRStep(mapper_init=self.mapper_init_parental_edu, # Use separate mapper_init
                   mapper=self.map_parental_edu,
                   reducer=self.reduce_parental_edu),
            MRStep(mapper_init=self.mapper_init_parental_edu_detailed, # Use separate mapper_init
                   mapper=self.map_parental_edu,
                   reducer=self.reduce_parental_edu_detailed),
            MRStep(mapper_init=self.mapper_init_absence_impact, # Use separate mapper_init
                   mapper=self.map_absence_impact,
                   reducer=self.reduce_absence_impact),
            MRStep(mapper_init=self.mapper_init_school_support, # Use separate mapper_init
                   mapper=self.map_school_support,
                   reducer=self.reduce_school_support),
            MRStep(mapper_init=self.mapper_init_study_time, # Use separate mapper_init
                   mapper=self.map_study_time,
                   reducer=self.reduce_study_time),
            MRStep(mapper_init=self.mapper_init_failure_analysis, # Use separate mapper_init
                   mapper=self.map_failure_analysis,
                   reducer=self.reduce_failure_analysis),
            MRStep(mapper_init=self.mapper_init_alcohol_consumption, # Use separate mapper_init
                   mapper=self.map_alcohol_consumption,
                   reducer=self.reduce_alcohol_consumption),
            MRStep(mapper_init=self.mapper_init_school_comparison, # Use separate mapper_init
                   mapper=self.map_school_comparison,
                   reducer=self.reduce_school_comparison),
            MRStep(mapper_init=self.mapper_init_romantic_impact, # Use separate mapper_init
                   mapper=self.map_romantic_impact,
                   reducer=self.reduce_romantic_impact),
            MRStep(mapper_init=self.mapper_init_age_performance, # Use separate mapper_init
                   mapper=self.map_age_performance,
                   reducer=self.reduce_age_performance)
        ]

    def mapper_init_gender_grades(self):
        """Initialize the CSV reader for the gender grades mapper."""
        self.csv_reader_gender_grades = csv.reader(self.stdin, delimiter=';')
        self.header_gender_grades = next(self.csv_reader_gender_grades)

    def mapper_init_parental_edu(self):
        """Initialize the CSV reader for the parental education mapper."""
        self.csv_reader_parental_edu = csv.reader(self.stdin, delimiter=';')
        self.header_parental_edu = next(self.csv_reader_parental_edu)

    def mapper_init_parental_edu_detailed(self):
        """Initialize the CSV reader for the parental education detailed mapper."""
        self.csv_reader_parental_edu_detailed = csv.reader(self.stdin, delimiter=';')
        self.header_parental_edu_detailed = next(self.csv_reader_parental_edu_detailed)

    def mapper_init_absence_impact(self):
        """Initialize the CSV reader for the absence impact mapper."""
        self.csv_reader_absence_impact = csv.reader(self.stdin, delimiter=';')
        self.header_absence_impact = next(self.csv_reader_absence_impact)

    def mapper_init_school_support(self):
        """Initialize the CSV reader for the school support mapper."""
        self.csv_reader_school_support = csv.reader(self.stdin, delimiter=';')
        self.header_school_support = next(self.csv_reader_school_support)

    def mapper_init_study_time(self):
        """Initialize the CSV reader for the study time mapper."""
        self.csv_reader_study_time = csv.reader(self.stdin, delimiter=';')
        self.header_study_time = next(self.csv_reader_study_time)

    def mapper_init_failure_analysis(self):
        """Initialize the CSV reader for the failure analysis mapper."""
        self.csv_reader_failure_analysis = csv.reader(self.stdin, delimiter=';')
        self.header_failure_analysis = next(self.csv_reader_failure_analysis)

    def mapper_init_alcohol_consumption(self):
        """Initialize the CSV reader for the alcohol consumption mapper."""
        self.csv_reader_alcohol_consumption = csv.reader(self.stdin, delimiter=';')
        self.header_alcohol_consumption = next(self.csv_reader_alcohol_consumption)

    def mapper_init_school_comparison(self):
        """Initialize the CSV reader for the school comparison mapper."""
        self.csv_reader_school_comparison = csv.reader(self.stdin, delimiter=';')
        self.header_school_comparison = next(self.csv_reader_school_comparison)

    def mapper_init_romantic_impact(self):
        """Initialize the CSV reader for the romantic impact mapper."""
        self.csv_reader_romantic_impact = csv.reader(self.stdin, delimiter=';')
        self.header_romantic_impact = next(self.csv_reader_romantic_impact)

    def mapper_init_age_performance(self):
        """Initialize the CSV reader for the age performance mapper."""
        self.csv_reader_age_performance = csv.reader(self.stdin, delimiter=';')
        self.header_age_performance = next(self.csv_reader_age_performance)

    def parse_record(self, record):
        """Parse a record, handling potential errors."""
        try:
            # Convert string values to integers where needed.
            gender = record[0]
            g1 = int(record[25])
            g2 = int(record[26])
            g3 = int(record[27])
            medu = int(record[11])
            fedu = int(record[12])
            absences = int(record[22])
            schoolsup = record[15]
            studytime = int(record[14])
            failures = int(record[16])
            address = record[8]
            traveltime = int(record[4])
            school = record[1]
            romantic = record[21]
            age = int(record[0])
            dalc = int(record[19])
            walc = int(record[20])
            return (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc)
        except ValueError as e:
            # Log the error and return None to skip this record.
            self.log.error(f"ValueError: {e}, Record: {record}")
            return None
        except IndexError as e:
            self.log.error(f"IndexError: {e}, Record: {record}")
            return None

    # 4. Impact of Absences on Performance
    def map_absence_impact(self, _, line):
        """Mapper function for analyzing the impact of absences on student performance."""
        try:
            record = next(self.csv_reader_absence_impact)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            # Yielding absences as the key and performance (g1, g2, g3) as values
            yield absences, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_absence_impact(self, absences, list_of_values):
        """Reducer function to calculate the impact of absences on performance."""
        sum_g1 = 0
        sum_g2 = 0
        sum_g3 = 0
        total_count = 0
        for g1, g2, g3, count in list_of_values:
            sum_g1 += g1
            sum_g2 += g2
            sum_g3 += g3
            total_count += count
        avg_g1 = sum_g1 / total_count if total_count > 0 else 0
        avg_g2 = sum_g2 / total_count if total_count > 0 else 0
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield absences, (avg_g1, avg_g2, avg_g3)

if __name__ == '__main__':
    StudentPerformanceAnalysis.run()
