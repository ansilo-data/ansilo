import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { AuthMappings } from "../../../components/governance/auth-mappings/AuthMappings";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Mappings">
      <AuthMappings />
    </Template>
  );
};

export default Page;
