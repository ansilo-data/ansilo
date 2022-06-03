import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { DataFlow } from "../../../components/operations/data-flow/DataFlow";

const Page: NextPage = () => {
  return (
    <Template title="Operations > Data Flow">
      <DataFlow />
    </Template>
  );
};

export default Page;
