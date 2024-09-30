CREATE TABLE IF NOT EXISTS "2024_michelle_bidart_schema".country (
    name VARCHAR(255) PRIMARY KEY,
    code CHAR(2)
);

CREATE TABLE IF NOT EXISTS "2024_michelle_bidart_schema".team (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    country VARCHAR(255) REFERENCES "2024_michelle_bidart_schema".country(name),
    logo VARCHAR(255),
    stadium_id INT
);

CREATE TABLE IF NOT EXISTS "2024_michelle_bidart_schema".venue (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    city VARCHAR(255),
    capacity INT,
    address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS "2024_michelle_bidart_schema".league (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    type VARCHAR(50),
    logo VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS "2024_michelle_bidart_schema".match (
    id INT PRIMARY KEY,
    date DATE,
    timezone VARCHAR(50),
    referee VARCHAR(255),
    league_id INT REFERENCES "2024_michelle_bidart_schema".league(id),
    venue_id INT REFERENCES "2024_michelle_bidart_schema".venue(id),
    status VARCHAR(50),
    team_home_id INT REFERENCES "2024_michelle_bidart_schema".team(id),
    team_away_id INT REFERENCES "2024_michelle_bidart_schema".team(id),
    home_score INT,
    away_score INT,
    penalty_home INT,
    penalty_away INT
);
