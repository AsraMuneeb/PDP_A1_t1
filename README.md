  <header style="background-color: #4CAF50; color: white; padding: 20px 10px; text-align: center;">
        <h1>Student Fee Analysis </h1>
        <p>Analyzing fee submission patterns for students</p>
    </header>
    <section style="padding: 20px; margin: 20px auto; max-width: 800px; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
        <h2 style="color: #4CAF50;">Overview</h2>
        <p>
            This project aims to identify the most frequent day of the month students submit their fees,
            based on a dataset of students and their payment records. The system supports both
            linear and parallel implementations to optimize execution time.
        </p>
    </section>
    <section style="padding: 20px; margin: 20px auto; max-width: 800px; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
        <h2 style="color: #4CAF50;">Setup Instructions</h2>
        <p>Follow these steps to set up and run the project:</p>
        <ol>
            <li>
                <strong>Create a virtual environment:</strong>
                <pre style="background-color: #f4f4f4; padding: 10px; border-left: 3px solid #4CAF50; font-family: monospace;">python -m venv venv</pre>
            </li>
            <li>
                <strong>Activate the virtual environment:</strong>
                <pre style="background-color: #f4f4f4; padding: 10px; border-left: 3px solid #4CAF50; font-family: monospace;">

Windows: venv\Scripts\activate

Mac/Ubuntu: source venv/bin/activate
                </pre>
            </li>
            <li>
                <strong>Install dependencies:</strong>
                <pre style="background-color: #f4f4f4; padding: 10px; border-left: 3px solid #4CAF50; font-family: monospace;">pip install -r requirements.txt</pre>
            </li>
            <li>
                <strong>Run the script linearly:</strong>
                <pre style="background-color: #f4f4f4; padding: 10px; border-left: 3px solid #4CAF50; font-family: monospace;">python linear.py</pre>
            </li>
            <li>
                <strong>Run the script parallel:</strong>
                <pre style="background-color: #f4f4f4; padding: 10px; border-left: 3px solid #4CAF50; font-family: monospace;">python parallel.py</pre>
            </li>
        </ol>
    </section>
    <section style="padding: 20px; margin: 20px auto; max-width: 800px; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
        <h2 style="color: #4CAF50;">Execution Time Comparison</h2>
        <p>The project implements two approaches:</p>
        <ol>
            <li><strong>Linear Implementation:</strong> Processes data sequentially, suitable for smaller datasets.</li>
            <li><strong>Parallel Implementation:</strong> Leverages multithreading to optimize performance on larger datasets.</li>
        </ol>
        <div style="display: flex; justify-content: space-between; margin-top: 20px;">
            <div style="width: 45%; padding: 10px; text-align: center; background-color: #e8f5e9; border: 1px solid #4CAF50; border-radius: 5px;">
                <h3 style="color: #4CAF50;">Linear Execution</h3>
                <p>Start Time: <strong>2024-12-10 19:42:38.056700</strong></p>
                <p>End Time: <strong>2024-12-10 19:42:56.945347</strong></p>
                <p><strong>Total Time:</strong> 18.88 seconds</p>
            </div>
            <div style="width: 45%; padding: 10px; text-align: center; background-color: #e8f5e9; border: 1px solid #4CAF50; border-radius: 5px;">
                <h3 style="color: #4CAF50;">Parallel Execution</h3>
                <p>Start Time: <strong>2024-12-10 19:47:44.715000</strong></p>
                <p>End Time: <strong>2024-12-10 19:47:48.288306</strong></p>
                <p><strong>Total Time:</strong> 03.57 seconds</p>
            </div>
        </div>
    </section>
</body>
</html>
