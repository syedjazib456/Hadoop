from mrjob.job import MRJob
from mrjob.step import MRStep
import csv  # To handle CSV data correctly

class StudentPerformanceAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_gender_grades,
                   reducer=self.reduce_gender_grades),
            MRStep(mapper=self.map_parental_edu,
                   reducer=self.reduce_parental_edu),
            MRStep(mapper=self.map_parental_edu,
                   reducer=self.reduce_parental_edu_detailed),  # Added detailed
            MRStep(mapper=self.map_absence_impact,
                   reducer=self.reduce_absence_impact),
            MRStep(mapper=self.map_school_support,
                   reducer=self.reduce_school_support),
            MRStep(mapper=self.map_study_time,
                   reducer=self.reduce_study_time),
            MRStep(mapper=self.map_failure_analysis,
                   reducer=self.reduce_failure_analysis),
            MRStep(mapper=self.map_alcohol_consumption,
                   reducer=self.reduce_alcohol_consumption),
            MRStep(mapper=self.map_school_comparison,
                   reducer=self.reduce_school_comparison),
            MRStep(mapper=self.map_romantic_impact,
                   reducer=self.reduce_romantic_impact),
            MRStep(mapper=self.map_age_performance,
                   reducer=self.reduce_age_performance)
        ]

    def mapper_init(self):
        """Initialize the CSV reader for each mapper."""
        self.csv_reader = csv.reader(self.stdin, delimiter=';')
        self.header = next(self.csv_reader)  # Skip the header row
    
    def parse_record(self, record):
        """Parse a record, handling potential errors."""
        try:
            # Convert string values to integers where needed.
            # print(record)
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
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield gender, (g1, g2, g3, 1)

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
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        parental_edu = "Medu" + str(medu) + "_Fedu" + str(fedu)
        yield parental_edu, (g3, 1)

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
        std_dev = variance ** 0.5 if variance > 0 else 0  #handle the case where variance is 0
        yield parental_edu, (avg_g3, min_g3, max_g3, std_dev)

    # 3. Absence Impact Analysis
    def map_absence_impact(self, _, line):
        """Mapper function for analyzing the impact of absences on grades."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        if absences < 5:
            absence_range = "0-4"
        elif absences < 10:
            absence_range = "5-9"
        else:
            absence_range = "10+"
        yield absence_range, (g3, 1)

    def reduce_absence_impact(self, absence_range, list_of_values):
        """Reducer function for analyzing the impact of absences on grades."""
        sum_g3 = 0
        total_count = 0
        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield absence_range, avg_g3

    # 4. School Support Effectiveness
    def map_school_support(self, _, line):
        """Mapper function for evaluating school support effectiveness."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield schoolsup, (g3, 1)

    def reduce_school_support(self, schoolsup, list_of_values):
        """Reducer function for evaluating school support effectiveness."""
        sum_g3 = 0
        total_count = 0
        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield schoolsup, avg_g3

    # 5. Study Time Analysis
    def map_study_time(self, _, line):
        """Mapper function for analyzing how study time affects grades."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield studytime, (g1, g2, g3, 1)

    def reduce_study_time(self, studytime, list_of_values):
        """Reducer function for analyzing how study time affects grades."""
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
        """Mapper function for understanding characteristics of students with past failures."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield failures, (g3, 1, address, traveltime)

    def reduce_failure_analysis(self, failures, list_of_values):
        """Reducer function for understanding characteristics of students with past failures."""
        sum_g3 = 0
        total_count = 0
        addresses = set()
        traveltimes = set()
        for g3, count, address, traveltime in list_of_values:
            sum_g3 += g3
            total_count += count
            addresses.add(address)
            traveltimes.add(traveltime)
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield failures, (avg_g3, list(addresses), list(traveltimes))

    # 7. Weekend vs. Weekday Alcohol Consumption
    def map_alcohol_consumption(self, _, line):
        """Mapper for comparing effects of alcohol consumption patterns."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        weekday_alc = "Low" if dalc <= 2 else "High"
        weekend_alc = "Low" if walc <= 2 else "High"
        key = weekday_alc + "_" + weekend_alc
        yield key, (g3, 1)

    def reduce_alcohol_consumption(self, key, list_of_values):
        """Reducer for comparing effects of alcohol consumption patterns."""
        sum_g3 = 0
        total_count = 0
        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield key, avg_g3

    # 8. School Comparison (GP vs MS)
    def map_school_comparison(self, _, line):
        """Mapper for comparing performance between schools."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield school, (g1, g2, g3, 1)

    def reduce_school_comparison(self, school, list_of_values):
        """Reducer for comparing performance between schools."""
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
        """Mapper for analyzing how romantic relationships affect performance."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield romantic, (g3, 1)

    def reduce_romantic_impact(self, romantic, list_of_values):
        """Reducer for analyzing how romantic relationships affect performance."""
        sum_g3 = 0
        total_count = 0
        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield romantic, avg_g3

    # 10. Age Performance Analysis
    def map_age_performance(self, _, line):
        """Mapper for understanding how age affects academic performance."""
        record = next(self.csv_reader)
        parsed_record = self.parse_record(record)
        if parsed_record is None:
            return  # Skip invalid records
        (gender, g1, g2, g3, medu, fedu, absences, schoolsup, studytime, failures, address, traveltime, school, romantic, age, dalc, walc) = parsed_record
        yield age, (g3, 1)

    def reduce_age_performance(self, age, list_of_values):
        """Reducer for understanding how age affects academic performance."""
        sum_g3 = 0
        total_count = 0
        for g3, count in list_of_values:
            sum_g3 += g3
            total_count += count
        avg_g3 = sum_g3 / total_count if total_count > 0 else 0
        yield age, avg_g3

if __name__ == '__main__':
    StudentPerformanceAnalysis.run()