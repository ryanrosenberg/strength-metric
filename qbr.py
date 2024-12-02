from openskill.models import PlackettLuce
import sqlite3 as sq
import pandas as pd

qbr = sq.connect('qbr.db')
model = PlackettLuce()

def run_game(date, tournament_id, tournament_round, pg, tg, game_id):
    print(f'{tournament_round}: {game_id}')
    player_stats = pg[pg['game_id'] == game_id]    
    team_stats = tg[tg['game_id'] == game_id]
    team_ids = team_stats['team_id'].unique()
    team1 = []
    team1_scores = []
    team1_stats = player_stats[player_stats['team_id'] == team_ids[0]]
    for i, row in team1_stats.iterrows():
        ratings = pd.read_sql(f'SELECT * from player_ratings where player_id = {int(row["player_id"])}', qbr)
        if len(ratings) > 0:
            p = model.rating(name = row['player_id'], mu = ratings.at[-1, 'mu'], sigma = ratings.at[-1, 'sigma'])
        else:
            p = model.rating(name = row['player_id'])
        team1.append(p)
        team1_scores.append(row['pts'])
    team2 = []
    team2_scores = []
    team2_stats = player_stats[player_stats['team_id'] == team_ids[1]]
    for i, row in player_stats[player_stats['team_id'] == team_ids[1]].iterrows():
        ratings = pd.read_sql(f'SELECT * from player_ratings where player_id = {int(row["player_id"])}', qbr)
        if len(ratings) > 0:
            p = model.rating(name = row['player_id'], mu = ratings.at[-1, 'mu'], sigma = ratings.at[-1, 'sigma'])
        else:
            p = model.rating(name = row['player_id'])
        team2.append(p)
        team2_scores.append(row['pts'])
    match = [team1, team2]
    [team1, team2] = model.rate(match, scores = list(team_stats['total_pts']), weights=[team1_scores, team2_scores])
    team1_entries = pd.DataFrame([
        {
            'date': date,
            'tournament_id': tournament_id,
            'round': tournament_round,
            'game_id': game_id,
            'team_id': list(team1_stats['team_id'])[i],
            'player_id': int(list(team1_stats['player_id'])[i]),
            'mu': player_rating.mu,
            'sigma': player_rating.sigma
        }
        for i, player_rating in enumerate(team1)
    ])
    team2_entries = pd.DataFrame([
        {
            'date': date,
            'tournament_id': tournament_id,
            'round': tournament_round,
            'game_id': game_id,
            'team_id': list(team2_stats['team_id'])[i],
            'player_id': int(list(team2_stats['player_id'])[i]),
            'mu': player_rating.mu,
            'sigma': player_rating.sigma
        }
        for i, player_rating in enumerate(team2)
    ])
    team1_entries.to_sql('player_ratings', qbr, if_exists='append', index = False)
    team2_entries.to_sql('player_ratings', qbr, if_exists='append', index = False)
    