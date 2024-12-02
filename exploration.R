library(tidyverse)
library(DBI)
library(reticulate)

con <- DBI::dbConnect(RSQLite::SQLite(), 'stats2.db')
qbr <- DBI::dbConnect(RSQLite::SQLite(), 'qbr.db')

source_python('qbr.py')

pg <- dbReadTable(con, "player_games") %>% 
  as_tibble()
tg <- dbReadTable(con, "team_games") %>% 
  as_tibble()
player_list <- dbReadTable(con, "players") %>% 
  left_join(dbReadTable(con, "people")) %>% 
  as_tibble()
teams <- dbReadTable(con, "teams")

gid <- pg %>% 
  slice_sample(n = 1) %>% 
  pull(game_id) %>% 
  pluck(1)

players <- pg %>% 
  filter(tournament_id == 2045)

teams <- tg %>% 
  filter(tournament_id == 2045)

dbCreateTable(qbr, 'player_ratings', )

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
    tournament_id == 2045,
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


calculate_game_score <- function(player_df, total_pts = 200){
  print(player_df$game_id[[1]])
  game_scores <- tbl(strength, "player_ratings") %>% 
    group_by(player_id) %>% 
    summarize(psr = mean(game_score)) %>% 
    collect()

  game <- player_df %>% 
    left_join(game_scores, by = join_by(player_id)) %>% 
    mutate(psr = coalesce(psr, 25)) %>% 
    mutate(
      exp_pts = total_pts*psr/sum(psr),
      game_score = pts-exp_pts
    )
  dbWriteTable(strength, "player_ratings", game, append = T)
}

nats %>% 
  select(date:player, pts) %>% 
  # filter(round %in% c("Round 1", "Round 2")) %>% 
  group_split(game_id) %>% 
  map(calculate_game_score)



