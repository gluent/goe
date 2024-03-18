# GOE Tests

## Unit Tests

```
pytest tests/unit
```


## Integration Tests

To be able to run integration tests locally you need to create a GOE_TEST user in your source Database, for example:

```
CREATE USER goe_test IDENTIFIED BY my_secret_123
DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO goe_test;
```

```
export GOE_TEST_USER_PASS=my_secret_123
pytest tests/integration
```
