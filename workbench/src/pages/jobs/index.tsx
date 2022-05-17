import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../components/template/Template";
import { JobsList } from "../../components/jobs/list/JobsList";

const Page: NextPage = () => {
  return (
    <Template title="Jobs">
      <JobsList />
    </Template>
  );
};

export default Page;
