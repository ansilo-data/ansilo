import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { Users } from "../../../components/governance/users/Users";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Users">
      <Users />
    </Template>
  );
};

export default Page;
