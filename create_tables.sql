DROP SCHEMA IF EXISTS pitch_data CASCADE;
CREATE SCHEMA pitch_data;

DROP TABLE IF EXISTS pitch_data.at_bats;
CREATE TABLE pitch_data.at_bats (
  id SERIAL PRIMARY KEY,
  ab_id INTEGER,
  game_id VARCHAR(60),
  batter INTEGER,
  pitcher INTEGER,
  ab_des TEXT,
  CONSTRAINT game_ab UNIQUE (ab_id, game_id)
);

DROP TABLE IF EXISTS pitch_data.pitches;
CREATE TABLE pitch_data.pitches (
  pitch_id SERIAL PRIMARY KEY,
  ab_id INTEGER,
  game_id VARCHAR(60),
  play_guid VARCHAR(60),
  start_speed NUMERIC,
  end_speed NUMERIC,
  pfx_x NUMERIC,
  px NUMERIC,
  sz_bot NUMERIC,
  ay NUMERIC,
  vy0 NUMERIC,
  break_angle NUMERIC,
  z0 NUMERIC,
  vz0 NUMERIC NULL,
  ax NUMERIC,
  y NUMERIC,
  type VARCHAR(25),
  sv_id VARCHAR(20),
  spin_rate NUMERIC,
  mt VARCHAR(50) NULL,
  type_confidence NUMERIC,
  y0 NUMERIC,
  des VARCHAR(255),
  vx0 NUMERIC,
  break_y NUMERIC,
  az NUMERIC,
  sz_top NUMERIC,
  spin_dir NUMERIC,
  zone INTEGER,
  pz NUMERIC,
  tfs INTEGER,
  x0 NUMERIC,
  tfs_zulu TIMESTAMP WITH TIME ZONE,
  event_num INTEGER,
  break_length NUMERIC,
  x NUMERIC,
  cc VARCHAR(255) NULL,
  nasty INTEGER,
  id INTEGER,
  pitch_type VARCHAR(4),
  pfx_z NUMERIC
);

DROP SCHEMA IF EXISTS tweet_data CASCADE;
CREATE SCHEMA tweet_data;

DROP TABLE IF EXISTS tweet_data.tweets;
CREATE TABLE tweet_data.tweets (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP,
  text VARCHAR(160),
  friends_count INTEGER,
  user_favs INTEGER,
  screen_name VARCHAR(140),
  user_id INTEGER,
  user_bio VARCHAR(160)
);

