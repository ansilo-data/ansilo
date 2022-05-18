import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { Authorities } from "../../../components/governance/authorities/Authorities";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Authorities">
      <Authorities />
    </Template>
  );
};

export default Page;
