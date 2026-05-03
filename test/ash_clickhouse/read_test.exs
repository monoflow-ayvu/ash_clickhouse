defmodule AshClickhouse.ReadTest do
  use AshClickhouse.RepoCase, async: true
  use Machete
  alias AshClickhouse.Test.Resource.User
  alias TestRepo

  require Ash.Query

  setup do
    TestRepo.query("TRUNCATE TABLE users")
    TestRepo.query("TRUNCATE TABLE organizations")
    TestRepo.query("TRUNCATE TABLE users_organizations")
    :ok
  end

  describe "User resource read tests" do
    test "simple read returns all records" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      VALUES
        (generateUUIDv4(), 'User1', 'user1@example.com', 21, 51.0, true, now(), now()),
        (generateUUIDv4(), 'User2', 'user2@example.com', 22, 52.0, true, now(), now()),
        (generateUUIDv4(), 'User3', 'user3@example.com', 23, 53.0, true, now(), now())
      """)

      users = User |> Ash.read!()

      assert users
             ~> in_any_order([
               struct_like(User, %{
                 name: "User1",
                 email: "user1@example.com",
                 age: 21,
                 score: 51.0,
                 is_active: true,
                 inserted_at: datetime(),
                 updated_at: datetime()
               }),
               struct_like(User, %{
                 name: "User2",
                 email: "user2@example.com",
                 age: 22,
                 score: 52.0,
                 is_active: true,
                 inserted_at: datetime(),
                 updated_at: datetime()
               }),
               struct_like(User, %{
                 name: "User3",
                 email: "user3@example.com",
                 age: 23,
                 score: 53.0,
                 is_active: true,
                 inserted_at: datetime(),
                 updated_at: datetime()
               })
             ])
    end

    test "read with filter returns matching records" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      VALUES
        (generateUUIDv4(), 'Alice', 'alice@example.com', 30, 95.0, true, now(), now()),
        (generateUUIDv4(), 'Bob', 'bob@example.com', 25, 85.0, false, now(), now())
      """)

      active_users =
        User
        |> Ash.Query.filter(is_active == true)
        |> Ash.read!()

      assert active_users
             ~> in_any_order([
               struct_like(User, %{
                 name: "Alice",
                 email: "alice@example.com",
                 age: 30,
                 is_active: true,
                 inserted_at: datetime(),
                 updated_at: datetime()
               })
             ])
    end

    test "read with limit returns N records" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      SELECT
        generateUUIDv4(),
        'User' || toString(number),
        'user' || toString(number) || '@example.com',
        20 + number,
        50.0 + number,
        true,
        now(),
        now()
      FROM numbers(10)
      """)

      limited_users =
        User
        |> Ash.Query.limit(5)
        |> Ash.read!()

      assert limited_users
             ~> list(
               length: 5,
               elements:
                 struct_like(User, %{
                   name: string(),
                   id: string(),
                   is_active: true,
                   inserted_at: datetime(),
                   updated_at: datetime()
                 })
             )
    end

    test "read with offset skips N records" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      SELECT
        generateUUIDv4(),
        'User' || leftPad(toString(number), 2, '0'),
        'user' || toString(number) || '@example.com',
        20 + number,
        50.0 + number,
        true,
        now(),
        now()
      FROM numbers(10)
      """)

      TestRepo.query("SYSTEM FLUSH ASYNC INSERT QUERIES")

      offset_users =
        User
        |> Ash.Query.sort(:name)
        |> Ash.Query.limit(5)
        |> Ash.Query.offset(5)
        |> Ash.read!()

      assert Enum.map(offset_users, & &1.name) == [
               "User05",
               "User06",
               "User07",
               "User08",
               "User09"
             ]
    end

    test "read with sort ascending returns sorted records" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      VALUES
        (generateUUIDv4(), 'Charlie', 'charlie@example.com', 35, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Alice', 'alice@example.com', 25, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Bob', 'bob@example.com', 30, 50.0, true, now(), now())
      """)

      sorted_asc =
        User
        |> Ash.Query.sort(:name)
        |> Ash.read!()

      assert Enum.map(sorted_asc, & &1.name) == ["Alice", "Bob", "Charlie"]
    end

    test "read with sort descending returns sorted records" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      VALUES
        (generateUUIDv4(), 'Charlie', 'charlie@example.com', 35, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Alice', 'alice@example.com', 25, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Bob', 'bob@example.com', 30, 50.0, true, now(), now())
      """)

      sorted_desc =
        User
        |> Ash.Query.sort(name: :desc)
        |> Ash.read!()

      assert length(sorted_desc) == 3
      assert Enum.map(sorted_desc, & &1.name) == ["Charlie", "Bob", "Alice"]
    end

    test "read with multiple sort columns" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      VALUES
        (generateUUIDv4(), 'Alice', 'alice_30@example.com', 30, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Bob', 'bob_25@example.com', 25, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Alice', 'alice_25@example.com', 25, 50.0, true, now(), now()),
        (generateUUIDv4(), 'Bob', 'bob_30@example.com', 30, 50.0, true, now(), now())
      """)

      sorted =
        User
        |> Ash.Query.sort([:name, :age])
        |> Ash.read!()

      assert length(sorted) == 4
      [alice25, alice30, bob25, bob30] = sorted
      assert alice25.name == "Alice" and alice25.age == 25
      assert alice30.name == "Alice" and alice30.age == 30
      assert bob25.name == "Bob" and bob25.age == 25
      assert bob30.name == "Bob" and bob30.age == 30
    end

    test "read with count" do
      TestRepo.query("""
      INSERT INTO users (id, name, email, age, score, is_active, inserted_at, updated_at)
      SELECT
        generateUUIDv4(),
        'User' || toString(number),
        'user' || toString(number) || '@example.com',
        20 + number,
        50.0 + number,
        true,
        now(),
        now()
      FROM numbers(7)
      """)

      assert Ash.count!(User) == 7
    end
  end
end
