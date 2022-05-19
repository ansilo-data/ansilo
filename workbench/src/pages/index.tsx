import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../components/template/Template";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";

const Home: NextPage = () => {
  return (
    <Template title="Test">
      <Container
        sx={{ py: 2, display: "flex", flexDirection: "column", flexGrow: 1 }}
      >
        <Paper
          sx={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            flexGrow: 1,
            mx: "auto",
            p: 4,
          }}
          elevation={8}
        >
          Select a menu item from the left to get started.
        </Paper>
      </Container>
    </Template>
  );
};

export default Home;
