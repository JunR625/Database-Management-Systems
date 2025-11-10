-- Create roles
CREATE ROLE viewer_role;
CREATE ROLE buyer_role;

-- Grant privileges
GRANT SELECT ON TABLE amazon.user_review TO viewer_role;
GRANT SELECT, UPDATE, DELETE ON TABLE amazon.user_review TO buyer_role;

-- Create users and assign roles (example for 10 users each)
-- Adjust usernames and passwords accordingly
DO
$$
BEGIN
  FOR i IN 1..1000 LOOP
    EXECUTE format('CREATE USER IF NOT EXISTS viewer_user_%s PASSWORD ''viewerpass%s''', i, i);
    EXECUTE format('GRANT viewer_role TO viewer_user_%s', i);
    EXECUTE format('CREATE USER IF NOT EXISTS buyer_user_%s PASSWORD ''buyerpass%s''', i, i);
    EXECUTE format('GRANT buyer_role TO buyer_user_%s', i);
  END LOOP;
END;
$$;
