import csv
from datetime import datetime
from collections import defaultdict

class FeeAnalysis:
    def _init_(self, students_file, fees_file):
        self.students_file = students_file
        self.fees_file = fees_file
        self.current_year = datetime.now().year
        self.students_data = []
        self.fees_by_student = defaultdict(list)
        self.output_data = []

    def load_students_data(self):
        """Load student data from CSV file."""
        with open(self.students_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.students_data = list(reader)

    def load_fees_data(self):
        """Load fee data from CSV file and organize by Student ID."""
        with open(self.fees_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for fee in reader:
                try:
                    if fee["Payment Status"].strip().lower() == "paid":
                        fee_date = datetime.strptime(f"{fee['Payment Date']} {self.current_year}", "%B %d %Y")
                        self.fees_by_student[fee["Student ID"].strip()].append(fee_date)
                except ValueError as e:
                    print(f"Skipping invalid date: {fee['Payment Date']} - Error: {e}")

    def analyze_fees(self):
        """Analyze the fees data to find the most frequent fee submission day."""
        for student in self.students_data:
            stud_id = student["Student ID"].strip()
            student_fee_dates = self.fees_by_student.get(stud_id, [])

            if student_fee_dates:
                day_frequencies = defaultdict(int)
                for date in student_fee_dates:
                    day_of_month = date.day
                    day_frequencies[day_of_month] += 1

                max_day = max(day_frequencies, key=day_frequencies.get)
                max_frequency = day_frequencies[max_day]

                self.output_data.append({
                    "Student ID": stud_id,
                    "Most Frequent Fee Submission Day": max_day,
                    "Frequency": max_frequency
                })

    def display_results(self):
        """Display the analysis results."""
        print("Execution Results:")
        for entry in self.output_data:
            print(f"Student ID: {entry['Student ID']}, "
                  f"Most Frequent Day: {entry['Most Frequent Fee Submission Day']}, "
                  f"Frequency: {entry['Frequency']}")

    def run_analysis(self):
        """Run the complete analysis pipeline."""
        start_time = datetime.now()

        self.load_students_data()
        self.load_fees_data()
        self.analyze_fees()

        end_time = datetime.now()
        execution_time = end_time - start_time

        self.display_results()

        print("\nExecution Timing:")
        print(f"Start Time: {start_time}")
        print(f"End Time: {end_time}")
        print(f"Total Execution Time: {execution_time}")

if _name_ == "_main_":
    students_file = 'data/students.csv'
    fees_file = 'data/student_fees.csv'

    fee_analysis = FeeAnalysis(students_file, fees_file)
    fee_analysis.run_analysis()
