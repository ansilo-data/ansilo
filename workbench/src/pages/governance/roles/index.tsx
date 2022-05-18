import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { Roles } from "../../../components/governance/roles/Roles";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Authorities">
      <Roles />
    </Template>
  );
};

export default Page;
