# TODO: REPLACE WITH PROJECT

Welcome to BuildFlow!

If you want to just get started quickly you can run start your project with:

```
buildflow run
```

Then you can visit http://localhost:8000 to see your project running.

## Directory Structure

At the root level there are three important files:

- `buildflow.yml` - This is the main configuration file for your project. It contains all the information about your project and how it should be built.
- `main.py` - This is the entry point to your project and where your `Flow` is initialized.
- `requirements.txt` - This is where you can specify any Python dependencies your project has.

Below the root level we have:

**TODO REPLACE WITH PROJECT DIRECTORY**

This is the directory where your project code lives. You can put any files you want in here and they will be available to your project. We create a couple directories for you:

- **processors**: This is where you can put any custom processors you want to use in your project. In here you will see we have defined a *service.py* for a service in your project and a *hello_world.py* file for a custom endpoint processor.
- **resources**: This is where you can define any custom primitive resources that your project will need. Note it is empty right now since your initial project is so simple.

**.buildflow**

This is a hidden directory that contains all the build artifacts for your project. You can general ignore this directory and it will be automatically generated for you. If you are using github you probably want to put this in your *.gitignore* file.
