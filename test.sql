CREATE USER viewer_user_1;
CREATE USER viewer_user_2;
CREATE USER viewer_user_3;
-- ... up to viewer_user_1000 if needed

CREATE USER buyer_user_1;
CREATE USER buyer_user_2;
-- ... similarly for buyers

GRANT SELECT ON TABLE user_review TO viewer_user_1;
GRANT SELECT ON TABLE user_review TO viewer_user_2;
-- etc.

GRANT SELECT, UPDATE, DELETE ON TABLE user_review TO buyer_user_1;
GRANT SELECT, UPDATE, DELETE ON TABLE user_review TO buyer_user_2;
-- etc.


SELECT username FROM system.users;
