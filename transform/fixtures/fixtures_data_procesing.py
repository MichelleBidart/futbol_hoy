import pandas as pd

csv_file_path = "./temp/extract/20240921_211816_temp.csv"
output_path = "./temp/transform"

df = pd.read_csv(csv_file_path)

df_filtered = df[df['league_id'] == 128]
print(df_filtered)

df_fixture = df_filtered[[
    'fixture_id', 'fixture_referee', 'fixture_timezone', 'fixture_date', 'fixture_timestamp', 
    'period_first', 'period_second', 'venue_id','status_long', 'status_short', 
    'status_elapsed', 'league_id', 'home_team_id', 'away_team_id'
]]
df_venue = df_filtered[['venue_id', 'venue_name', 'venue_city']]
print(df_venue)

df_league = df_filtered[['league_id', 'league_name', 'league_country']]


df_team_home = df_filtered[['home_team_id', 'home_team_name', 'home_team_logo']].rename(
    columns={'home_team_id': 'team_id', 'home_team_name': 'team_name', 'home_team_logo': 'team_logo'}
)
df_team_away = df_filtered[['away_team_id', 'away_team_name', 'away_team_logo']].rename(
    columns={'away_team_id': 'team_id', 'away_team_name': 'team_name', 'away_team_logo': 'team_logo'}
)
df_team = pd.concat([df_team_home, df_team_away]).drop_duplicates()


df_score = df_filtered[[
    'fixture_id', 'home_goals', 'away_goals', 'halftime_home', 'halftime_away',
    'fulltime_home', 'fulltime_away', 'extratime_home', 'extratime_away', 'penalty_home', 'penalty_away'
]]


df_status = df_filtered[['status_short', 'status_long']].drop_duplicates().reset_index(drop=True)
df_status['status_id'] = df_status.index + 1  


df_fixture = df_fixture.merge(df_status, on=['status_short', 'status_long'])

df_fixture.to_csv(f"{output_path}/fixture.csv", index=False)
df_venue.to_csv(f"{output_path}/venue.csv", index=False)
df_league.to_csv(f"{output_path}/league.csv", index=False)
df_team.to_csv(f"{output_path}/team.csv", index=False)
df_score.to_csv(f"{output_path}/score.csv", index=False)
df_status.to_csv(f"{output_path}/status.csv", index=False)
