import dataclasses
from typing import List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row

import buildflow as flow


@dataclasses.dataclass
class Person:
    # ID is a special type used to index the rows in the table. It
    # autoincrements with every new row. It should be used when no PrimaryKey
    # is provided.
    id: flow.ID() = None
    # PrimaryKey is a special type that is used to index the rows in the table.
    name: flow.PrimaryKey(str) = ''
    # All base python types are supported as columns
    age: int = 0
    hobbies: List[str] = dataclasses.field(default_factory=list)
    birthday: datetime = dataclasses.field(default_factory=datetime)


postgres = flow.Postgres(user='postgres',
                         password='postgres',
                         host='localhost:5455',
                         database='postgres')


@flow.storage(provider=postgres, schema=Person, table_name='persons')
def write_person(person: Person, session: Session) -> int:
    session.add(person)
    session.commit()


@flow.storage(provider=postgres)
def get_person_using_primary_keys(id: int, name: str,
                                  session: Session) -> Person:
    # You can grab individual objects using their ID.
    return session.get(Person, (id, name))


@flow.storage(provider=postgres)
def list_persons_using_orm(session: Session) -> List[Person]:
    # We bind the dataclass to the orm session so that we can use the class to
    # quickly query the table.
    return session.query(Person).all()


@flow.storage(provider=postgres)
def list_persons_using_sql(session: Session) -> List[Row]:
    # You can also run arbitrary queries on the table.
    return session.execute('SELECT * FROM persons').all()


person = Person(name='Robby',
                age=33,
                hobbies=['golf', 'hiking'],
                birthday=datetime(1985, 7, 25))

# Write the person object to a Postgres table
write_person(person)

# Fetch the person object from the table using the primary keys: (id, name)
print('Get person by ID:')
print(get_person_using_primary_keys(person.id, person.name))
print('')

# List all persons in the table using the ORM
print('ORM Results:')
for person in list_persons_using_orm():
    print(person)
print('')

# List all persons in the table using the SQL
print('SQL Results:')
for person in list_persons_using_sql():
    print(person)
