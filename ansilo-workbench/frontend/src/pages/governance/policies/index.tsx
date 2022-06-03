import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { Policies } from "../../../components/governance/policies/Policies";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Authorities">
      <Policies />
    </Template>
  );
};

export default Page;
