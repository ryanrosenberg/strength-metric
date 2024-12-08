library(tidyverse)
library(DBI)
library(reticulate)
library(caret)

con <- DBI::dbConnect(RSQLite::SQLite(), '../add-tournament/add-tournament/stats2.db')
qbr <- DBI::dbConnect(RSQLite::SQLite(), 'qbr.db')
strength <- DBI::dbConnect(RSQLite::SQLite(), 'strength.db')

source_python('qbr.py')

pg <- dbReadTable(con, "player_games") %>% 
  as_tibble()
tg <- dbReadTable(con, "team_games") %>% 
  as_tibble()
player_list <- dbReadTable(con, "players") %>% 
  left_join(dbReadTable(con, "people")) %>% 
  as_tibble()
teams <- dbReadTable(con, "teams")
tournaments <- dbReadTable(con, "tournaments")
gid <- pg %>% 
  filter(tournament_id == 2045) %>% 
  slice_sample(n = 1) %>% 
  pull(game_id) %>% 
  pluck(1)

nats_players <- pg %>% 
  left_join(dbReadTable(con, "tournaments") %>% distinct()) %>% 
  left_join(dbReadTable(con, "sets")) %>% 
  filter(difficulty != 'easy', as.numeric(str_sub(year, end = 2)) >= 2) %>% 
  left_join(tournaments) %>% 
  select(-player) %>% 
  left_join(player_list)

nats_teams <- tg %>% 
  filter(tournament_id == 2045) %>% 
  left_join(tournaments)

points_buffer <- 20

initial_variability <- 2
variability_dec <- .85
variability_min <- .05
var_daily_increase <- .001

initial_qbr <- 100

calculate_game_score <- function(player_df, total_pts = 180){
  cur_tournament <- ''
  if(player_df$tournament_name[[1]] != cur_tournament){
    print(glue::glue('{player_df$tournament_name[[1]]}'))
    cur_tournament <- player_df$tournament_name[[1]]
  }
  player_ids <- player_df$player_id
  person_ids <- player_df$person_id
  game_scores <- tbl(strength, "player_ratings_2") %>%
    filter(player_id %in% player_ids | person_id %in% person_ids) %>% 
    collect() %>% 
    group_by(player) %>% 
    slice(n()) %>% 
    ungroup() %>% 
    mutate(qbr = pmax((old_qbr + adjustment), 0))

  game <- player_df %>%
    select(-tournament_name) %>% 
    left_join(game_scores %>% 
                select(person_id, last_date = date, qbr, variability), 
              by = join_by(person_id)) %>% 
    mutate(
      days_since = ymd(date) - ymd(last_date),
      qbr = coalesce(qbr, initial_qbr),
      variability = (variability*variability_dec)+(var_daily_increase*as.numeric(days_since)),
      variability = pmin(variability, initial_variability),
      variability = pmax(variability, variability_min),
      variability = coalesce(variability, initial_variability)
      ) %>%
    mutate(
      exp_pts = total_pts*(qbr + points_buffer)/sum(qbr + points_buffer),
      game_score = pts-exp_pts,
      adjustment = game_score/(1/variability)
    )
  dbWriteTable(strength, "player_ratings_2", 
               game %>% rename(old_qbr = qbr), append = T)
  # return(game)
}

dbRemoveTable(strength, "player_ratings_2")
dbCreateTable(strength, "player_ratings_2", nats_players %>% 
                select(date, tournament_id, round, game_id, team_id, player_id, person_id, player, pts) %>% 
                mutate(old_qbr = NA, variability = NA, exp_pts = NA, 
                       game_score = NA, adjustment = NA,
                       last_date = NA, days_since = NA))

tictoc::tic()
nats_players %>% 
  select(date, tournament_id, tournament_name, 
         round, game_id, team_id, 
         player_id, person_id, player, pts) %>% 
  arrange(date, game_id) %>% 
  # filter(round %in% c("Round 1", "Round 2")) %>%
  group_split(date, game_id) %>% 
  walk(calculate_game_score)
tictoc::toc()

dbReadTable(strength, "player_ratings_2") %>% 
  as_tibble() %>%
  filter(player %in% c("Rob Carson", "Andrew Hart", "Billy Busse", "Tejas Raje")) %>%
  group_by(date, game_id) %>% 
  mutate(renum = cur_group_id()) %>% 
  ungroup() %>% 
  ggplot(aes(x = renum, y = old_qbr, group = player, color = player)) + 
  geom_line(linewidth = 2) +
  scale_fill_manual(values = wesanderson::wes_palette('GrandBudapest2')) +
  scale_color_manual(values = wesanderson::wes_palette('GrandBudapest2')) +
  labs(y = 'Rating') +
  theme_bw() +
  theme(
    panel.border = element_blank(),
    # axis.text.x = element_text(angle = 60, vjust = 1, hjust = 1),
    axis.text.x = element_blank(),
    axis.ticks = element_blank()
  )

ratings <- dbReadTable(strength, "player_ratings_2") %>% 
  as_tibble()

ratings %>% 
  group_by(year = year(ymd(date))) %>% 
  summarize(
    p10 = quantile(old_qbr, .1),
    p25 = quantile(old_qbr, .25),
    p50 = quantile(old_qbr, .5),
    p75 = quantile(old_qbr, .75),
    p90 = quantile(old_qbr, .9),
    p95 = quantile(old_qbr, .95),
  ) %>% 
  gather(quantile, qbr, -year) %>% 
  ggplot(aes(year, qbr, group = quantile)) +
  geom_line(aes(color = quantile), linewidth = 2) +
  scale_x_continuous(breaks = 2011:2024) +
  theme_bw() +
  theme(panel.grid.minor = element_blank())

tg %>% 
  filter(tournament_id == 1642)

team_strength <- ratings %>%
  left_join(pg %>% 
              distinct(tournament_id, game_id, player_id, tuh)) %>% 
  left_join(tg %>% 
              distinct(tournament_id, game_id, team_id, team_tuh = tuh, 
                       result, total_pts, opp_pts)) %>% 
  left_join(tournaments) %>% 
  left_join(dbReadTable(con, "sets")) %>% 
  mutate(across(c(tuh, team_tuh), \(x)coalesce(x, default_tuh))) %>% 
  arrange(player) %>% 
  group_by(date, tournament_id, round, game_id, team_id, result, total_pts, opp_pts) %>% 
  summarize(n_players = n(),
            total_qbr = sum(old_qbr*(tuh/team_tuh)),
            na_players = sum(is.na(player)),
            players = paste(glue::glue('{player} ({round(old_qbr)})'), collapse = ', ')) %>% 
  ungroup()

qbr_results <- team_strength %>% 
  filter(!is.na(result)) %>% 
  group_by(date, tournament_id, round, game_id) %>% 
  mutate(team = row_number(), opp_qbr = rev(total_qbr), .after = total_qbr) %>% 
  mutate(opp_players = rev(players)) %>% 
  ungroup() %>% 
  mutate(qbr_result = case_when(total_qbr > opp_qbr ~ 1, total_qbr == opp_qbr ~ .5, T ~ 0),
         margin = total_pts - opp_pts,
         qbr_margin = total_qbr - opp_qbr,
         .after = result)

qbr_tab <- qbr_results %>% 
  filter(across(result:qbr_result, ~.!=.5), team == 1) %>% 
  mutate(across(result:qbr_result, factor)) %>% 
  group_split(year(date))

qbr_tab %>% 
  map_dbl(\(x)confusionMatrix(x$qbr_result, x$result)$overall[['Accuracy']])

qbr_results %>% 
  ggplot(aes(qbr_margin, margin)) +
  geom_point() +
  facet_wrap(~na_players) +
  theme_bw()

confusionMatrix(qbr_tab %>% 
                  reduce(bind_rows) %>% 
                  pull(qbr_result), 
                qbr_tab %>% 
                  reduce(bind_rows) %>% 
                  pull(result))

team_strength %>% 
  group_by(team_id, tournament_id, players) %>% 
  summarize(avg_qbr = mean(total_qbr))

tg %>% 
  mutate(team_id = ifelse(prov_team == "UW Cringe" & tournament_id == 1659, '756-2', team_id),
         opponent_id = ifelse(opponent == "UW Cringe" & tournament_id == 1659, '756-2', opponent_id)) %>% 
  # filter(tournament_id == 880) %>% 
  dbWriteTable(con, "team_games", ., overwrite = T)
tg %>% 
  left_join(dbReadTable(con, "tournaments")) %>% 
  left_join(dbReadTable(con, "sets")) %>% 
  mutate(alt_date = mdy(date), date = ymd(date), date = coalesce(alt_date, date)) %>% 
  distinct(year, date, tournament_id, tournament_name, round, game_id) %>% 
  arrange(date, game_id) %>% 
  filter(year == '23-24') %>%
  select(date, tournament_id, round, game_id) %>% 
  filter(tournament_id == 2018) %>% 
  # slice(1:5) %>% 
  pwalk(function(date, tournament_id, round, game_id){
    run_game(date, tournament_id, round, 
      pg %>% 
      filter(tournament_id == 2018), 
    tg %>% 
        filter(tournament_id == 2018), game_id)
  })

ratings <- dbReadTable(qbr, "player_ratings") %>% 
  as_tibble() %>% 
  left_join(player_list) %>% 
  left_join(teams)

ratings %>% 
  filter(
    player %in% c(
    "Anuttam Ramji",
    "Rohan Shelke",
    "Robert Condron",
    "John Marvin"
  )) %>% 
  left_join(dbReadTable(con, "tournaments")) %>% 
  select(date, tournament_name, tournament_id:game_id, player, mu, sigma) %>% 
  group_by(date, game_id) %>% 
  mutate(renum = cur_group_id()) %>% 
  ungroup() %>% 
  # left_join(tg %>% 
  #   filter(team_id == '126-2') %>% 
  #   select(game_id, opponent)) %>% 
  # mutate(
  #   opponent = factor(opponent),
  #   opponent = reorder(opponent, game_id)
  # ) %>% 
  ggplot(aes(renum, mu, color = player, group = player)) +
  geom_line(linewidth = 2) +
  geom_ribbon(
    aes(ymin = mu-1.28*sigma, ymax = mu+1.28*sigma, fill = player),
    alpha = .3
  ) +
  scale_fill_manual(values = wesanderson::wes_palette('GrandBudapest2')) +
  scale_color_manual(values = wesanderson::wes_palette('GrandBudapest2')) +
  labs(y = 'Rating') +
  theme_bw() +
  theme(
    panel.border = element_blank(),
    # axis.text.x = element_text(angle = 60, vjust = 1, hjust = 1),
    axis.text.x = element_blank(),
    axis.ticks = element_blank()
  )

ratings %>% 
  left_join(dbReadTable(con, "tournaments")) %>% 
  select(date, tournament_name, game_id, team, person_id, player, rating = mu, variance = sigma) %>% 
  write_csv('23-24-test-trueskill-ratings.csv')

dbReadTable(con, "tournaments") %>% 
  as_tibble() %>% 
  mutate(date = as.character(ymd('1970-01-01') + days(date))) %>% 
  dbWriteTable(con, "tournaments", ., overwrite = T)

pg %>% 
  group_by(tournament_id, set_id, game_id) %>% 
  summarize(total_tossup_points = sum(pts, na.rm = T)) %>% 
  left_join(dbReadTable(con, "sets")) %>% 
  group_by(set_id, set, difficulty) %>% 
  summarize(med_tu_pts = median(total_tossup_points))





