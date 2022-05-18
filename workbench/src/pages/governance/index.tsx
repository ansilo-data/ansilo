import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../components/template/Template";
import { Authorities } from "../../components/governance/authorities/Authorities";
import { GovernanceIndex } from "../../components/governance/GovernanceIndex";

const Page: NextPage = () => {
  return (
    <Template title="Governance">
      <GovernanceIndex />
    </Template>
  );
};

export default Page;
