**Installation and Running Guide: Crisis Data Pipeline Of Natural Disaster**

# Prerequisites
1. Install Docker Desktop.
2. Install Python 3.8 or higher.
3. Install the required chatbot libraries: `pip install discord.py psycopg2 requests folium pandas`

# How to Run the System

# Step 1: Start the Airflow and Database Servers
1. Open your Terminal (or Command Prompt) and navigate to the project folder.
2. Run the command: `docker-compose up -d`
3. Wait until all containers are fully up and running.

# Step 2: Configure the Database Connection in Airflow
1. Open your web browser and go to: `http://localhost:8080` (Username/Password: admin/admin).
2. Go to the **Admin** menu -> **Connections**.
3. Click the `+` button to add a new connection and fill in the following details:
   - Connection Id: `my_postgres_conn`
   - Connection Type: `Postgres`
   - Host: `postgres`
   - Schema: `postgres`
   - Login: `airflow`
   - Password: `airflow`
   - Port: `5432`
4. Click **Save**.

# Step 3: Start the Data Pipeline
1. On the Airflow web interface, go to the **DAGs** menu.
2. Search for the DAG named: `disaster_postgres_full_pipeline`.
3. Toggle the switch to unpause the DAG (it will turn blue).
4. Click the **Trigger DAG (Play button)** to run the system immediately.

# Step 4: Launch the Discord Chatbot
1. Insert your Discord Bot Token into the `TOKEN` variable inside the `disaster_bot.py` file.
2. Open a new Terminal window and run the command: `python disaster_bot.py`
3. The bot will instantly come online in Discord. You can now use the command `/check [province_name]` directly in the chat channel.