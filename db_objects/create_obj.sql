CREATE TABLE IF NOT EXISTS rooms (
    id SERIAL PRIMARY KEY,
    name VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS students (
    id SERIAL,
    birthday TIMESTAMP,
    name VARCHAR(50),
    room INTEGER,
    sex VARCHAR(1),
    CONSTRAINT fk_id
      FOREIGN KEY(id)
	  REFERENCES rooms(id)
  );