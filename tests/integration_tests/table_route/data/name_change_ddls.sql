USE route_name_src;

ALTER TABLE alt_rename RENAME TO alt_renamed;
INSERT INTO alt_renamed VALUES (2, 'after alter rename');

RENAME TABLE multi_a TO multi_a_new, multi_b TO multi_b_new;
INSERT INTO multi_a_new VALUES (2, 'multi_a_after');
INSERT INTO multi_b_new VALUES (2, 'multi_b_after');

RENAME TABLE cross_move TO route_name_extra.cross_moved;
INSERT INTO route_name_extra.cross_moved VALUES (2, 'cross_move_after');

RENAME TABLE swap_a TO swap_tmp, swap_b TO swap_a, swap_tmp TO swap_b;
INSERT INTO swap_a VALUES (3, 'swap_a_after');
INSERT INTO swap_b VALUES (4, 'swap_b_after');

DROP TABLE drop_1, drop_2;

DROP TABLE recreate_old;
CREATE TABLE recreate_old (id INT PRIMARY KEY, note VARCHAR(64));
INSERT INTO recreate_old VALUES (2, 'recreated');

RENAME TABLE route_name_extra.extra_seed TO route_name_extra.extra_seed_new;
INSERT INTO route_name_extra.extra_seed_new VALUES (2, 'extra_seed_after');

CREATE TABLE finish_name_change (id INT PRIMARY KEY);
INSERT INTO finish_name_change VALUES (1);
