import subprocess
from queue import Queue
import threading

# Configuration
WORKING_DIR = "../../crons/weekly/world"
COUNTRIES = [
    'us', 'gbrofs', 'aut', 'bel', 'dnk', 'fin', 'fra', 'deu',
    'irl', 'ita', 'nld', 'nor', 'prt', 'esp', 'swe', 'che'
]
SCRIPTS = [
    'world_mgt.py',
    'world_overview.py',
    'world_portfolio.py',
    'world_perf.py',
    'world_rr.py'
]
THREADS_PER_COUNTRY = 8
MAX_THREADS = 128  # 16 countries * 8 threads

def run_script(script, country, threads):
    """Run a single script with the given parameters"""
    cmd = f"cd {WORKING_DIR} && python3 {script} {country} {threads}"
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True)
        return (script, country, True, "OK")
    except subprocess.CalledProcessError as e:
        return (script, country, False, "OK")

def worker(task_queue, result_queue):
    """Worker thread that processes tasks from the queue"""
    while True:
        task = task_queue.get()
        if task is None:  # Sentinel value to stop the worker
            break
        script, country = task
        result = run_script(script, country, THREADS_PER_COUNTRY)
        result_queue.put(result)
        task_queue.task_done()

def manage_workflow():
    """Manage the entire workflow with dynamic task assignment"""
    task_queue = Queue()
    result_queue = Queue()
    
    # Create and start worker threads
    workers = []
    for _ in range(MAX_THREADS):
        t = threading.Thread(target=worker, args=(task_queue, result_queue))
        t.start()
        workers.append(t)
    
    # Track which scripts each country has completed
    country_status = {country: set() for country in COUNTRIES}
    
    # Function to get the next script for a country
    def get_next_script(country):
        completed = country_status[country]
        for script in SCRIPTS:
            if script not in completed:
                return script
        return None
    
    # Initial population of the queue
    for country in COUNTRIES:
        script = get_next_script(country)
        if script:
            task_queue.put((script, country))
    
    # Process results and add new tasks as countries complete
    active_countries = set(COUNTRIES)
    while active_countries:
        script, country, success, message = result_queue.get()
        
        # Update status
        if success:
            print(f"{script} {country} completed successfully")
            country_status[country].add(script)
        else:
            print(f"{script} {country} failed: {message}")
            # You might want to retry failed jobs here
        
        # Check if country has more work
        next_script = get_next_script(country)
        if next_script:
            task_queue.put((next_script, country))
        else:
            active_countries.discard(country)
            print(f"Country {country} completed all scripts")
    
    # Stop workers
    for _ in range(MAX_THREADS):
        task_queue.put(None)
    for t in workers:
        t.join()
    
    print("All work completed")

if __name__ == "__main__":
    print("Starting optimized parallel execution")
    manage_workflow()