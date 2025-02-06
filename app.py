import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Connect to SQLite database
conn = sqlite3.connect('data/machine.sqlite')

# Query data from the database
query = "SELECT * FROM streamed_messages"
df = pd.read_sql_query(query, conn)

# Convert unix timestamp to datetime with UTC timezone
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

# Close the database connection
conn.close()

# Streamlit app
st.set_page_config(layout="wide")
st.title('Machine Data Dashboard')

# Line graphs side by side
col1, col2, col3 = st.columns(3)

# Line graph for temperature over time
with col1:
    st.subheader('Temperature Over Time')
    fig, ax = plt.subplots()
    for machine_id, group in df.groupby('machine_id'):
        ax.plot(group['timestamp'], group['temperature'], label=f'Machine {machine_id}')
    ax.set_xlabel('Time')
    ax.set_ylabel('Temperature')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=45)
    st.pyplot(fig)

# Line graph for RPM over time
with col2:
    st.subheader('RPM Over Time')
    fig, ax = plt.subplots()
    for machine_id, group in df.groupby('machine_id'):
        ax.plot(group['timestamp'], group['rpm'], label=f'Machine {machine_id}')
    ax.set_xlabel('Time')
    ax.set_ylabel('RPM')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=45)
    st.pyplot(fig)

# Line graph for conveyor speed over time
with col3:
    st.subheader('Conveyor Speed Over Time')
    fig, ax = plt.subplots()
    for machine_id, group in df.groupby('machine_id'):
        ax.plot(group['timestamp'], group['conveyor_speed'], label=f'Machine {machine_id}')
    ax.set_xlabel('Time')
    ax.set_ylabel('Conveyor Speed')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=45)
    st.pyplot(fig)

# Percent stacked bar chart for product quality
with col1:
    st.subheader('Product Quality by Machine')
    quality_counts = df.groupby(['machine_id', 'product_quality']).size().unstack(fill_value=0)
    quality_percent = quality_counts.div(quality_counts.sum(axis=1), axis=0) * 100  # Convert to percentage
    quality_percent.plot(kind='bar', stacked=True, color=['red', 'green'], ax=plt.gca())
    plt.xlabel('Machine ID')
    plt.ylabel('Percent')
    plt.ylim(0, 100)  # Scale y-axis from 0 to 100 percent
    plt.legend(['Bad', 'Good'])
    st.pyplot(plt.gcf())

# Pie chart for error codes
with col2:
    st.subheader('Error Code Distribution')
    error_counts = df['error_code'].value_counts()
    error_counts = error_counts[error_counts.index.notnull()]  # Exclude null values
    fig, ax = plt.subplots()
    ax.pie(error_counts, labels=error_counts.index, autopct='%1.1f%%', colors=plt.cm.tab20.colors)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    st.pyplot(fig)

# Bar chart for total widgets produced by machine
with col3:
    st.subheader('Total Widgets Produced by Machine')
    widget_counts = df.groupby('machine_id')['widgets_produced'].sum()
    fig, ax = plt.subplots()
    widget_counts.plot(kind='bar', ax=ax)
    ax.set_xlabel('Machine ID')
    ax.set_ylabel('Total Widgets Produced')
    ax.set_aspect('auto')  # Set aspect ratio to auto
    st.pyplot(fig)