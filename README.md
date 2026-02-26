# qb2pgsql

This is a tool for importing quality reports ("qb" for "Qualitätsberichten") from hospitals
in Germany into PostgreSQL. This data is published as XML files and to obtain it, you create
an account and send a request at this portal: https://qb-datenportal.g-ba.de/. More information
about the data itself can also be found at that portal.

> [!WARNING]
> This project currently only serves a very narrow use case of creating a list hospitals in
> Germany and whether these provide emergency medical services. But, these reports have tons of
> interesting data, so as the project expands, more can be included.
> 
> If you are interested in seeing more data extracted by this tool, please post exactly
> what as a feature request in the issues.

## A note about the data

This application has been written to use the reports from 2024 and is not guaranteed to work
with older reports. If you would like to add support for older report years, please submit
a feature request or suggest a change with a pull request.

## Usage

After obtaining a copy of the report and extracting it to a folder called, "data":

```
qb2pgsql data --host localhost --port 5432 --database my_database --schema hospitals --user dbuser
```

## Contributing

Want to help make this project better? Please file an issue with your ideas for improvements
in the issues.
