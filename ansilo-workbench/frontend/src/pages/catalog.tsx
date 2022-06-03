import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../components/template/Template";
import { Catalog } from "../components/catalog/Catalog";

const CatalogPage: NextPage = () => {
  return (
    <Template title="Data Catalog">
      <Catalog />
    </Template>
  );
};

export default CatalogPage;
