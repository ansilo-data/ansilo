import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../components/template/Template";
import { Sql } from "../components/sql/Sql";

const Catalog: NextPage = () => {
  return (
    <Template title="Workbench">
      <Sql />
    </Template>
  );
};

export default Catalog;
