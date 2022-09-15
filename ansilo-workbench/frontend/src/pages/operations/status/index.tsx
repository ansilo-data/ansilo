import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { Status } from "../../../components/operations/status/Status";

const Page: NextPage = () => {
  return (
    <Template title="Operations > Data Flow">
      <Status />
    </Template>
  );
};

export default Page;
