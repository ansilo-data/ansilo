import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../../components/template/Template";
import { Role } from "../../../../components/governance/roles/specific/Role";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Roles > Analyst">
      <Role />
    </Template>
  );
};

export default Page;
