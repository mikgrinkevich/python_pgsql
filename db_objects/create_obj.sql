CREATE TABLE IF NOT EXISTS rooms (
    id INTEGER PRIMARY KEY,
    name VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS students (
    birthday TIMESTAMP,
    id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    room INTEGER,
    sex VARCHAR(1),
    CONSTRAINT fk_id
      FOREIGN KEY(room)
	  REFERENCES rooms(id)
  );