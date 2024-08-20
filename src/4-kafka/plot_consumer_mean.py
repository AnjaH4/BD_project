import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
import json
from collections import defaultdict
import numpy as np

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'rolling_avg',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Create a figure and axis
fig, ax = plt.subplots()
data = defaultdict(lambda: {'x': [], 'y': []})  # Dictionary to store data for each day of the week

def init():
    ax.set_xlim(-0.5, 6.5)  # Adjust according to the number of days in a week
    ax.set_ylim(0, 400)  # Adjust based on expected mean values
    ax.set_xticks(range(7))  # 7 days of the week
    ax.set_xticklabels(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    ax.set_xlabel('Day of Week')
    ax.set_ylabel('Mean Tickets')
    return []

def update(frame):
    try:
        message = next(consumer)
        data_point = message.value
        day = data_point['day_of_week']
        week = int(data_point['week_of_year'])
        mean = data_point['mean_tickets']

        # Update data dictionary
        if week not in data[day]['x']:
            data[day]['x'].append(week)
            data[day]['y'].append(mean)
        else:
            idx = data[day]['x'].index(week)
            data[day]['y'][idx] = mean

        # Clear the current plot
        ax.clear()
        ax.set_xlim(-0.5, 6.5)  # Reset x-axis limits
        ax.set_ylim(0, 400)  # Reset y-axis limits
        ax.set_xticks(range(7))  # 7 days of the week
        ax.set_xticklabels(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
        ax.set_xlabel('Day of Week')
        ax.set_ylabel('Mean Tickets')

        # Prepare data for bar plot
        avg_mean_per_day = {day: np.mean(data[day]['y']) for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}

        days = list(avg_mean_per_day.keys())
        means = [avg_mean_per_day[day] for day in days]

        # Plot bars for each day of the week
        ax.bar(days, means)

        print(f"Updated plot with: Day {day}, Week {week}, Mean {mean}")

    except StopIteration:
        pass

    return []

ani = animation.FuncAnimation(fig, update, init_func=init, blit=False, interval=1000)  # Update every 1 second

plt.show()
