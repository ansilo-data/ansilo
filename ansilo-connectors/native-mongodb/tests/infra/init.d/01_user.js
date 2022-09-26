db.createUser({
  user: "ansilo_admin",
  pwd: "ansilo_testing",
  roles: [
    {
      role: "readWrite",
      db: "db",
    },
  ],
});
