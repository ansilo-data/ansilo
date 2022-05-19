import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../components/template/Template";
import { OperationsIndex } from "../../components/operations/OperationsIndex";

const Page: NextPage = () => {
  return (
    <Template title="Operations">
      <OperationsIndex />
    </Template>
  );
};

export default Page;
