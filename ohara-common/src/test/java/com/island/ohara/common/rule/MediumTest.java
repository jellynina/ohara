package com.island.ohara.common.rule;

import com.island.ohara.common.util.CommonUtil;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MediumTest {
  protected final Logger logger = LoggerFactory.getLogger(MediumTest.class);
  @Rule public final TestName name = new TestName();

  @Rule public final Timeout globalTimeout = new Timeout(5, TimeUnit.MINUTES);

  public String methodName() {
    return name.getMethodName();
  }

  public String random() {
    return CommonUtil.uuid();
  }
}