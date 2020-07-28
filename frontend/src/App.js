import React, { useState } from "react";
import {
  Box,
  Button,
  Container,
  Grid,
  Paper,
  Table,
  TableBody,
  TableContainer,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  ThemeProvider,
  Typography,
  createMuiTheme,
} from "@material-ui/core";
import { Parser } from "node-sql-parser";
import logo from "./logo.svg";
import "./App.css";

const axios = require("axios").default;

const theme = createMuiTheme({
  typography: {
    fontFamily: ["Roboto Mono", "monospace"].join(","),
  },
});

const COLUMNS = {
  staff: ["id", "first_name", "last_name", "email", "active", "store"],
  customer: ["id", "first_name", "last_name", "email", "city", "store"],
  product: ["id", "product_name", "list_price", "store"],
  orders: [
    "id",
    "customer_id",
    "order_date",
    "staff_id",
    "store",
    "order_status",
  ],
  order_items: ["id", "order_id", "product_id", "quantity", "store"],
  stocks: ["id", "product_id", "quantity", "store"],
};
const INSERT_URL = "http://localhost:5000/insert";
const SELECT_URL = "http://localhost:5000/select";

const App = () => {
  const parser = new Parser();
  const [sql, setSQL] = useState("");
  const [sqlError, setSQLError] = useState(false);
  const [sqlErrorMessage, setSQLErrorMessage] = useState("");
  const [textResp, setTextResp] = useState(null);
  const [records, setRecords] = useState([]);

  const parseSQL = () => {
    setTextResp("");
    setRecords([]);
    try {
      let ast = parser.astify(sql);
      switch (ast.type) {
        case "select":
          if (!ast.from) {
            setSQLError(true);
            setSQLErrorMessage(
              `Error: SELECT with no tables specified is not valid`
            );
            return;
          } else {
            // Reject statement with more than 1 table specified
            if (ast.from.length > 1) {
              setSQLError(true);
              setSQLErrorMessage(
                `Error: Only selects from 1 relationship are currently supported`
              );
              return;
            }

            // Reject statement where table specified is not staff
            if (
              ast.from[0].table.toLowerCase() != "staff" &&
              ast.from[0].table.toLowerCase() != "products" &&
              ast.from[0].table.toLowerCase() != "orders" &&
              ast.from[0].table.toLowerCase() != "customers" &&
              ast.from[0].table.toLowerCase() != "order_items" &&
              ast.from[0].table.toLowerCase() != "stocks"
            ) {
              setSQLError(true);
              setSQLErrorMessage(
                `Error: The relation "${ast.from[0].table}" does not exist`
              );
              return;
            }
            if (ast.columns != "*") {
              for (const column of ast.columns) {
                if (!COLUMNS[ast.from[0]].includes(column)) {
                  setSQLError(true);
                  setSQLErrorMessage(
                    `Error: Column "${column}" does not exist in "${ast.from[0]}"`
                  );
                  return;
                }
              }
            }
            let data = new FormData();
            data.set("sql", sql);
            axios({
              method: "POST",
              url: SELECT_URL,
              data: data,
              headers: { "Content-Type": "multipart/form-data" },
            })
              .then((res) => {
                if (res.status == 200) {
                  setRecords(res.data);
                } else {
                  setTextResp(res.data.status);
                }
              })
              .catch((err) => {
                setTextResp("An error occured while making the request");
                console.error(err);
              });
          }

          break;

        case "insert":
          if (ast.table[0].table.toLowerCase() != "staff") {
            setSQLError(true);
            setSQLErrorMessage(
              `Error: The relation "${ast.table[0].table}" does not exist`
            );
            return;
          } else {
            // Relation provided is staff
            // Check columns
            if (!ast.columns) {
              // Order them
            } else {
              if (ast.columns.length != ast.values[0].value.length) {
                setSQLError(true);
                setSQLErrorMessage(
                  `Error: INSERT has more target columns than expressions`
                );
                return;
              }
              for (const column of ast.columns) {
                if (!COLUMNS.includes(column)) {
                  setSQLError(true);
                  setSQLErrorMessage(
                    `Error: Column "${column}" does not exist`
                  );
                  return;
                }
              }
              // Create insert object
              let data = new FormData();
              ast.columns.forEach((column, i) => {
                data.set(column, ast.values[0].value[i].value);
              });
              axios
                .post(INSERT_URL, data, {
                  headers: { "Content-Type": "multipart/form-data" },
                })
                .then((res) => {
                  setTextResp(res.data.status);
                })
                .catch((err) => {
                  setTextResp("An error occured while making the request");
                  console.error(err);
                });
            }
          }

        default:
          break;
      }
    } catch (error) {
      setSQLError(true);
      setSQLErrorMessage(`${error.name}: ${error.message}`);
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <Container>
        <Box
          minHeight="100vh"
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
        >
          <Typography variant="h3" align="center">
            DDBMS
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <TextField
                variant="outlined"
                multiline
                label="SQL statement"
                rows={6}
                fullWidth
                margin="normal"
                onChange={(e) => setSQL(e.target.value)}
                error={sqlError}
                helperText={sqlErrorMessage}
              />
              <Box display="flex" justifyContent="end">
                <Button
                  variant="contained"
                  color="primary"
                  onClick={(e) => parseSQL()}
                  disableElevation
                >
                  Submit Query
                </Button>
              </Box>
            </Grid>
            <Grid item xs={12}>
              <Typography variant="h5">Results</Typography>
              <Box>
                {records.length > 0 && (
                  <TableContainer component={Paper}>
                    <Table>
                      <TableHead>
                        <TableRow>
                          {Object.keys(records[0]).map((column) => (
                            <TableCell>{column}</TableCell>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {records.map((record, index) => (
                          <TableRow key={index}>
                            {Object.keys(record).map((column) => (
                              <TableCell>
                                {new String(record[column])}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
                {textResp && <Typography>{textResp}</Typography>}
                {records.length < 1 && !textResp && (
                  <Typography>
                    Enter a statement and submit it to fetch results
                  </Typography>
                )}
              </Box>
            </Grid>
          </Grid>
        </Box>
      </Container>
    </ThemeProvider>
  );
};

export default App;
