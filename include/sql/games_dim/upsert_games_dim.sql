MERGE INTO games_dim as target
USING temp_games_dim as temp
ON target.game_id = temp.game_id
WHEN MATCHED THEN
    UPDATE SET
        target.game_name = temp.game_name,
        target.box_art_url = temp.box_art_url,
        target.igdb_id = temp.igdb_id
WHEN NOT MATCHED THEN
    INSERT (game_id, game_name, box_art_url, igdb_id)
    VALUES (
        temp.game_id,
        temp.game_name,
        temp.box_art_url,
        temp.box_art_url
    )