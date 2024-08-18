import faust
from collections import defaultdict
import datetime
import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score

def data_prep(batch):
    #onehot encode day_of_week and violation_county
    #onehot encode day_of_week
    day_of_week = batch['day_of_week']
    day_of_week = pd.get_dummies(day_of_week)
    #onehot encode violation_county
    violation_county = batch['violation_county']
    violation_county = pd.get_dummies(violation_county)
    #concatenate dataframes
    batch = pd.concat([day_of_week, violation_county, batch], axis=1)
    #drop day_of_week and violation_county columns
    batch = batch.drop(columns=['day_of_week', 'violation_county'])
    #check if all days are present (monday - sunday)
    #add missing columns
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
        if day not in batch.columns:
            batch[day] = 0

    #check if all counties are present (NY, K, Q, BX, R)
    #add missing columns
    for county in ['NY', 'K', 'Q', 'BX', 'R']:
        if county not in batch.columns:
            batch[county] = 0

    #set the order of columns
    batch = batch[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'NY', 'K', 'Q', 'BX', 'R', 'vehicle_year', 'violation_time', 'awnd', 'prcp', 'snow', 'tmax', 'tmin', 'schools', 'businesses', 'attractions']]

    #normalize numerical columns
    numericals = ['vehicle_year', 'violation_time', 'awnd', 'prcp', 'snow', 'tmax', 'tmin', 'schools', 'businesses', 'attractions']
    x = batch[numericals].values
    std_scaler = preprocessing.StandardScaler()
    x_scaled = std_scaler.fit_transform(x)
    df_temp = pd.DataFrame(x_scaled, columns=numericals, index=batch.index)
    batch[numericals] = df_temp

    #drop rows with NaN values
    batch = batch.dropna()

    batch = batch.replace({False: 0, True: 1})

    return batch

app = faust.App('stream-process_tickets', broker='kafka://localhost:29092', topic_partitions=8)
topic_tickets = app.topic('tickets', partitions=8)
topic_rolling_avg = app.topic('rolling_avg', partitions=8)
topic_day_sum = app.topic('day_sum', partitions=8)
topic_vehicle_make = app.topic('vehicle_make', partitions=8)
topic_rolling_stat_boroughs = app.topic('rolling_stat_boroughs', partitions=8)
topic_rolling_stat_streets = app.topic('rolling_stat_streets', partitions=8)

previous_week = app.Table('week', default=int, partitions=8)
previous_week.reset_state()

class RollingAvg(faust.Record):
    day_of_week: str
    week_of_year: str
    mean_tickets: float
    week_count: int

class DaySum(faust.Record):
    day_of_week: str
    sum_tickets: float

class WehicleMake(faust.Record):
    wehicle_make: str
    percentage: int

class ClusteringData(faust.Record):
    day_of_week: str  #onehotencode
    violation_county: str #onehotencode
    vehicle_year: int
    violation_time: int
    awnd: float
    prcp: float
    snow: float
    tmax: float
    tmin: float
    schools: float
    businesses: float
    attractions: float

class RollingStatBoroughs(faust.Record):
    day_of_week: str
    borough: str
    sum_tickets: int

class RollingStatStreets(faust.Record):
    day_of_week: str
    street: str
    sum_tickets: int


count_tickets_day = defaultdict(float)
mean_ticket_day = defaultdict(float)
#week_sums = defaultdict(float)
week_count_real = defaultdict(int)
passed_weeks = defaultdict(int)
day_week_counts = defaultdict(int)
wehicle_make_counts = defaultdict(int)

# Create a defaultdict to store lists of counts for each borough on each day
weekly_counts_borough = defaultdict(lambda: defaultdict(int))
weekly_counts_streets = defaultdict(lambda: defaultdict(int))

stats_day_week_counts = defaultdict(list)
max_tickets_date = defaultdict(int)

stats_day_week_counts_boroughs = defaultdict(lambda: defaultdict(list))
max_tickets_date_boroughs = defaultdict(lambda: defaultdict(int))

stats_day_week_counts_streets = defaultdict(lambda: defaultdict(list))
max_tickets_date_streets = defaultdict(lambda: defaultdict(int))

clustering_data = defaultdict(list)

week_count_real['count'] = 0

interesting_streets = ['Broadway',
 '3rd Ave',
 '5th Ave',
 'Madison Ave',
 '2nd Ave',
 'Lexington Ave',
 '1st Ave',
 'Queens Blvd',
 '7th Ave',
 '8th Ave']

"""""""""""""""""""""""""""
PROCESS TICKETS
"""""""""""""""""""""""""""

@app.agent(topic_tickets)  ##FINISHED
async def process_mean_tickets_day(tickets): #neki kar bi se updejtal enkrat na dan (konec dneva, začetek naslednjega
    #week_count = 0 #a je to ok da je tuki???
    async for ticket in tickets:
        old_week = previous_week['week']
        date_obj = datetime.datetime.strptime(ticket['Issue Date'], '%Y-%m-%d')
        day_of_week = date_obj.strftime('%A') #whole day name
        week_of_year = date_obj.strftime('%W')


        try:
            street = ticket['Street']
        except KeyError:
            street = ticket['Street Name']


        day_week_counts[day_of_week] += 1
        weekly_counts_borough[day_of_week][ticket['Violation County']] += 1

        if street in interesting_streets:
            weekly_counts_streets[day_of_week][street] += 1

        vehicle_make = ticket['Vehicle Make']
        wehicle_make_counts[vehicle_make] += 1

        #print(f'vehicle update')

        #caluclate percentage of wehicle make
        for make in wehicle_make_counts:
            percentage = wehicle_make_counts[make] / sum(wehicle_make_counts.values())
            vehicle_make_perc = WehicleMake(wehicle_make=make, percentage=percentage)
            await topic_vehicle_make.send(value=vehicle_make_perc)
            #print(f'Sent wehicle make: {vehicle_make}')

        #after each week send number per day to another kafka topic for calculating median and stdev !!!!!!!!!!!!!!!!!!!!
        if old_week != week_of_year and not passed_weeks[week_of_year]:
            week_count_real['count'] += 1 #to je treba za vsak dan posebi (lahko se začne s sredo --> pondelk, tork ne bosta mela istega št tedna

            for day in count_tickets_day:
                dsum = DaySum(day_of_week=day, sum_tickets=day_week_counts[day])
                await topic_day_sum.send(value=dsum)
                #print(f'Daily Sum: {dsum}')

                #set all values of day_week_counts to 0
                day_week_counts[day] = 0

            for day in weekly_counts_borough:
                for borough in weekly_counts_borough[day]:
                    r_stat = RollingStatBoroughs(day_of_week=day, borough=borough, sum_tickets=weekly_counts_borough[day][borough])
                    await topic_rolling_stat_boroughs.send(value=r_stat)
                    #print(f'Rolling stat boroughs: {r_stat}')

            for day in weekly_counts_streets:
                for street in weekly_counts_streets[day]:
                    r_stat = RollingStatStreets(day_of_week=day, street=street, sum_tickets=weekly_counts_streets[day][street])
                    await topic_rolling_stat_streets.send(value=r_stat)
                    #print(f'Rolling stat streets: {r_stat}')

        previous_week['week'] = week_of_year

        count_tickets_day[day_of_week] += 1
        passed_weeks[week_of_year] = True

"""""""""""""""""""""""""""""""""
ROLLING STATISTICS ALL DATA
"""""""""""""""""""""""""""""""""
@app.agent(topic_day_sum)
async def process_day_sum(day_sum):
    async for day in day_sum:
        #day_w = day['day_of_week']  #namest tak access morš dat day.day_od_week, ker je Faust record -> Object
        day_w = day.day_of_week
        day_sm = day.sum_tickets
        #day_sm = day['sum_tickets']
        stats_day_week_counts[day_w].append(day_sm)
        #calculate median and stdev
        median = np.median(stats_day_week_counts[day_w])
        stdev = np.std(stats_day_week_counts[day_w])
        mean = np.mean(stats_day_week_counts[day_w])

        if not max_tickets_date[day_w]:
            max_tickets_date[day_w] = day_sm

        elif day_sm > max_tickets_date[day_w]:
            max_tickets_date[day_w] = day_sm

        #print(f'Day: {day}, Median: {median}, Stdev: {stdev}, Mean: {mean}, Max tickets: {max_tickets_date[day.day_of_week]}')


"""""""""""""""""""""""""""""""""
ROLLING STATISTICS BOROUGHS
"""""""""""""""""""""""""""""""""
@app.agent(topic_rolling_stat_boroughs)
async def process_rolling_stat_boroughs(rolling_stat_boroughs):
    async for r_stat in rolling_stat_boroughs:
        day_w = r_stat.day_of_week
        borough = r_stat.borough
        sum_tickets = r_stat.sum_tickets
        stats_day_week_counts_boroughs[day_w][borough].append(sum_tickets)

        median = np.median(stats_day_week_counts_boroughs[day_w][borough])
        stdev = np.std(stats_day_week_counts_boroughs[day_w][borough])
        mean = np.mean(stats_day_week_counts_boroughs[day_w][borough])

        if not max_tickets_date_boroughs[day_w][borough]:
            max_tickets_date_boroughs[day_w][borough] = sum_tickets

        elif sum_tickets > max_tickets_date_boroughs[day_w][borough]:
            max_tickets_date_boroughs[day_w][borough] = sum_tickets

        #print(f'Day: {day_w}, Borough: {borough}, Median: {median}, Stdev: {stdev}, Mean: {mean}, Max tickets: {max_tickets_date_boroughs[day_w][borough]}')

        #print(f'Got rolling stat boroughs: {r_stat.day_of_week}, {r_stat.borough}, {r_stat.sum_tickets}')
"""""""""""""""""""""""""""""""""
ROLLING STATISTICS STREETS
"""""""""""""""""""""""""""""""""
@app.agent(topic_rolling_stat_streets)
async def process_rolling_stat_streets(rolling_stat_streets):
    async for r_stat in rolling_stat_streets:
        day_w = r_stat.day_of_week
        street = r_stat.street
        sum_tickets = r_stat.sum_tickets
        stats_day_week_counts_streets[day_w][street].append(sum_tickets)

        median = np.median(stats_day_week_counts_streets[day_w][street])
        stdev = np.std(stats_day_week_counts_streets[day_w][street])
        mean = np.mean(stats_day_week_counts_streets[day_w][street])

        if not max_tickets_date_streets[day_w][street]:
            max_tickets_date_streets[day_w][street] = sum_tickets

        elif sum_tickets > max_tickets_date_streets[day_w][street]:
            max_tickets_date_streets[day_w][street] = sum_tickets

        #print(f'Day: {day_w}, Street: {street}, Median: {median}, Stdev: {stdev}, Mean: {mean}, Max tickets: {max_tickets_date_streets[day_w][street]}')

"""""""""""""""""""""""""""""""""""
VEHICLE MAKE PERCENTAGE CONSUMER
"""""""""""""""""""""""""""""""""""
#vehicle make percentage
@app.agent(topic_vehicle_make)
async def process_vehicle_make(vehicle_make):
    async for make in vehicle_make:
        pass
        #print(f'Got wehicle make: {make.wehicle_make}, percentage: {make.percentage}')

"""""""""""""""""""""
CLUSTERING CONSUMER
"""""""""""""""""""""

@app.agent(topic_tickets)
async def clustering_data_prep(tickets):
    #batch naj bo 1000
    # define df: columns: monday tuesday wednesday thursday friday saturday  ny k Q bx r vehice_year violation_time awnd prcp snow tmax tmin schools businesses attractions
    #df = pd.DataFrame(columns=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'NY', 'K', 'Q', 'BX', 'R', 'vehicle_year', 'violation_time', 'awnd', 'prcp', 'snow', 'tmax', 'tmin', 'schools', 'businesses', 'attractions'])
    dbscan_model = DBSCAN(eps=0.5, min_samples=5)
    async for ticket in tickets:
        date_obj = datetime.datetime.strptime(ticket['Issue Date'], '%Y-%m-%d')
        day_of_week = date_obj.strftime('%A')
        violation_county = ticket['Violation County']
        vehicle_year = ticket['Vehicle Year']
        violation_time = ticket['Violation Time']

        if violation_time != '' :

            try:

                if '0' in violation_time:
                    violation_time = violation_time.replace('0', '')

                if 'P' in violation_time:
                    violation_time = violation_time.replace('P', '')
                    violation_time = int(violation_time) + 1200

                elif 'A' in violation_time:
                    violation_time = violation_time.replace('A', '')
                    violation_time = int(violation_time)
            except ValueError:
                continue

        awnd = ticket['AWND']
        prcp = ticket['PRCP']
        snow = ticket['SNOW']
        tmax = ticket['TMAX']
        tmin = ticket['TMIN']
        schools = ticket['Schools']
        businesses = ticket['Businesses']
        attractions = ticket['Attractions']

        clustering_data['data'].append([day_of_week, violation_county, vehicle_year, violation_time, awnd, prcp, snow, tmax, tmin, schools, businesses, attractions])

        #wait for 1000 tickets:
        if len(clustering_data['data']) == 1000:
            batch = pd.DataFrame(clustering_data['data'], columns=['day_of_week', 'violation_county', 'vehicle_year', 'violation_time', 'awnd', 'prcp', 'snow', 'tmax', 'tmin', 'schools', 'businesses', 'attractions'])
            prepped_batch = data_prep(batch)
            dbscan_model.fit(prepped_batch)
            labels = dbscan_model.labels_
            unique_labels = set(labels)
            clusters = {label: prepped_batch[labels == label] for label in unique_labels if label != -1}

            #print(f'Clusters: {clusters}')

            score = silhouette_score(prepped_batch, labels)
            print(f'Silhouette Score: {score}')
            print(f'Number of clusters: {len(clusters)}')
            #get the model
            #print(f'components: {dbscan_model.components_}')


            # Clear the buffer
            clustering_data.clear()

if __name__ == '__main__':
    app.main()


