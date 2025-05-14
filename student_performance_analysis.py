from mrjob.job import MRJob
from mrjob.step import MRStep
import csv  # To handle CSV data correctly

class StudentPerformanceAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init_gender_grades,
                   mapper=self.map_gender_grades,
                   reducer=self.reduce_gender_grades),
            MRStep(mapper_init=self.mapper_init_parental_edu,
                   mapper=self.map_parental_edu,
                   reducer=self.reduce_parental_edu),
            MRStep(mapper_init=self.mapper_init_parental_edu_detailed,
                   mapper=self.map_parental_edu,
                   reducer=self.reduce_parental_edu_detailed),
            MRStep(mapper_init=self.mapper_init_absence_impact,
                   mapper=self.map_absence_impact,
                   reducer=self.reduce_absence_impact),
            MRStep(mapper_init=self.mapper_init_school_support,
                   mapper=self.map_school_support,
                   reducer=self.reduce_school_support),
            MRStep(mapper_init=self.mapper_init_study_time,
                   mapper=self.map_study_time,
                   reducer=self.reduce_study_time),
            MRStep(mapper_init=self.mapper_init_failure_analysis,
                   mapper=self.map_failure_analysis,
                   reducer=self.reduce_failure_analysis),
            MRStep(mapper_init=self.mapper_init_alcohol_consumption,
                   mapper=self.map_alcohol_consumption,
                   reducer=self.reduce_alcohol_consumption),
            MRStep(mapper_init=self.mapper_init_school_comparison,
                   mapper=self.map_school_comparison,
                   reducer=self.reduce_school_comparison),
            MRStep(mapper_init=self.mapper_init_romantic_impact,
                   mapper=self.map_romantic_impact,
                   reducer=self.reduce_romantic_impact),
            MRStep(mapper_init=self.mapper_init_age_performance,
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

    # 1. Average Grades by Gender
    def map_gender_grades(self, _, line):
        """Mapper function for calculating average grades by gender."""
        try:
            record = next(self.csv_reader_gender_grades)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield gender, (g1, g2, g3, 1)
        except StopIteration:
            pass # Handle end of file

    def reduce_gender_grades(self, gender, list_of_values):
        """Reducer function for calculating average grades by gender."""
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
        yield gender, (avg_g1, avg_g2, avg_g3)

    # 2. Performance by Parental Education Level
    def map_parental_edu(self, _, line):
        """Mapper function for analyzing performance by parental education."""
        try:
            record = next(self.csv_reader_parental_edu)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            parental_edu = "Medu" + str(medu) + "_Fedu" + str(fedu)
            yield parental_edu, (g3, 1)
        except StopIteration:
            pass

    def reduce_parental_edu(self, parental_edu, list_of_values):
        """Reducer function for analyzing performance by parental education."""
        sum_g3 = 0
        total_count = 0
        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield parental_edu, avg_g3

    def reduce_parental_edu_detailed(self, parental_edu, list_of_values):
        """Reducer function to calculate avg, min, max, and std dev."""
        sum_g3 = 0
        total_count = 0
        min_g3 = float('inf')
        max_g3 = float('-inf')
        squared_sum = 0

        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
            min_g3 = min(min_g3, g3)
            max_g3 = max(max_g3, g3)
            squared_sum += g3 * g3

        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        variance = (squared_sum / total_count) - (avg_g3 * avg_g3) if total_count > 0 else 0
        std_dev = variance ** 0.5 if variance > 0 else 0  # handle the case where variance is 0
        yield parental_edu, (avg_g3, min_g3, max_g3, std_dev)

    # 3. Absence Impact Analysis
    def map_absence_impact(self, _, line):
        """Mapper function for analyzing the impact of absences on grades."""
        try:
            record = next(self.csv_reader_absence_impact)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
                       if absences >= 0:  # Ignore negative absences
                yield absences, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_absence_impact(self, absences, list_of_values):
        """Reducer function for analyzing the impact of absences on grades."""
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

    # 4. School Support Impact
    def map_school_support(self, _, line):
        """Mapper function for analyzing school support impact on grades."""
        try:
            record = next(self.csv_reader_school_support)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield schoolsup, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_school_support(self, schoolsup, list_of_values):
        """Reducer function for analyzing school support impact on grades."""
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
        yield schoolsup, (avg_g1, avg_g2, avg_g3)

    # 5. Study Time vs Performance
    def map_study_time(self, _, line):
        """Mapper function for analyzing the relationship between study time and grades."""
        try:
            record = next(self.csv_reader_study_time)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield studytime, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_study_time(self, studytime, list_of_values):
        """Reducer function for analyzing study time vs performance."""
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
        yield studytime, (avg_g1, avg_g2, avg_g3)

    # 6. Failure Analysis
    def map_failure_analysis(self, _, line):
        """Mapper function for analyzing the relationship between failures and performance."""
        try:
            record = next(self.csv_reader_failure_analysis)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield failures, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_failure_analysis(self, failures, list_of_values):
        """Reducer function for analyzing the impact of failures on performance."""
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
        yield failures, (avg_g1, avg_g2, avg_g3)

    # 7. Alcohol Consumption Impact
    def map_alcohol_consumption(self, _, line):
        """Mapper function for analyzing alcohol consumption impact on performance."""
        try:
            record = next(self.csv_reader_alcohol_consumption)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield dalc, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_alcohol_consumption(self, dalc, list_of_values):
        """Reducer function for analyzing the impact of alcohol consumption on grades."""
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
        yield dalc, (avg_g1, avg_g2, avg_g3)

    # 8. School Comparison Analysis
    def map_school_comparison(self, _, line):
        """Mapper function for analyzing performance comparison between schools."""
        try:
            record = next(self.csv_reader_school_comparison)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield school, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_school_comparison(self, school, list_of_values):
        """Reducer function for analyzing performance comparison between schools."""
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
        yield school, (avg_g1, avg_g2, avg_g3)

    # 9. Romantic Relationship Impact
    def map_romantic_impact(self, _, line):
        """Mapper function for analyzing the impact of romantic relationships on performance."""
        try:
            record = next(self.csv_reader_romantic_impact)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield romantic, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_romantic_impact(self, romantic, list_of_values):
        """Reducer function for analyzing the impact of romantic relationships on performance."""
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
        yield romantic, (avg_g1, avg_g2, avg_g3)

    # 10. Age vs Performance Analysis
    def map_age_performance(self, _, line):
        """Mapper function for analyzing the relationship between age and performance."""
        try:
            record = next(self.csv_reader_age_performance)
            parsed_record = self.parse_record(record)
            if parsed_record is None:
                return  # Skip invalid records
            (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
            yield age, (g1, g2, g3, 1)
        except StopIteration:
            pass

    def reduce_age_performance(self, age, list_of_values):
        """Reducer function for analyzing the relationship between age and performance."""
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
        yield age, (avg_g1, avg_g2, avg_g3)

if __name__ == '__main__':
    StudentPerformanceAnalysis.run()

