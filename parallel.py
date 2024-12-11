import dask.dataframe as dd
from datetime import datetime

class FeeAnalysisDask:
    def __init__(self, students_file, fees_file):
        self.students_file = students_file
        self.fees_file = fees_file
        self.current_year = datetime.now().year
        self.start_time = None
        self.end_time = None
        self.output_data = None

    def load_data(self):
        """Load students and fees data into Dask DataFrames."""
        self.students_df = dd.read_csv(self.students_file, encoding='utf-8')
        self.fees_df = dd.read_csv(self.fees_file, encoding='utf-8')

    def preprocess_fees_data(self):
        """Preprocess the fees data."""
        self.fees_df['Payment Status'] = self.fees_df['Payment Status'].str.strip().str.lower()
        self.fees_df['Payment Date'] = self.fees_df['Payment Date'].apply(
            lambda x: f"{x} {self.current_year}", meta=('x', 'str')
        )
        self.fees_df['Payment Date'] = dd.to_datetime(
            self.fees_df['Payment Date'], format='%B %d %Y', errors='coerce'
        )

    def analyze_fees(self):
        """Analyze fees to find the most frequent fee submission day."""
        paid_fees = self.fees_df[self.fees_df['Payment Status'] == 'paid']
        paid_fees['Day of Month'] = paid_fees['Payment Date'].dt.day

        day_frequency = paid_fees.groupby(['Student ID', 'Day of Month']).size().reset_index()
        day_frequency.columns = ['Student ID', 'Day of Month', 'Frequency']
        day_frequency = day_frequency.compute()

        most_frequent_days = (
            day_frequency.loc[
                day_frequency.groupby('Student ID')['Frequency'].idxmax()
            ]
        )

        self.students_df = self.students_df.compute()
        self.output_data = self.students_df.merge(
            most_frequent_days[['Student ID', 'Day of Month', 'Frequency']],
            on='Student ID',
            how='left'
        )

    def display_results(self):
        """Display the analysis results."""
        print("Execution Results:")
        print(self.output_data[['Student ID', 'Day of Month', 'Frequency']])

    def display_timing(self):
        """Display the execution timing."""
        execution_time = self.end_time - self.start_time
        print("\nExecution Timing:")
        print(f"Start Time: {self.start_time}")
        print(f"End Time: {self.end_time}")
        print(f"Total Execution Time: {execution_time}")

    def run_analysis(self):
        """Run the complete analysis pipeline."""
        self.start_time = datetime.now()

        self.load_data()
        self.preprocess_fees_data()
        self.analyze_fees()

        self.end_time = datetime.now()
        self.display_results()
        self.display_timing()

if __name__ == "__main__":
    students_file = 'data/students.csv'
    fees_file = 'data/student_fees.csv'

    fee_analysis = FeeAnalysisDask(students_file, fees_file)
    fee_analysis.run_analysis()
