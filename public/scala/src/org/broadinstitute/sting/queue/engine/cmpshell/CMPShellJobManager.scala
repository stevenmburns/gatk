/*
 * Copyright (c) 2011, The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.queue.engine.shell

import org.broadinstitute.sting.queue.function.CommandLineFunction
import org.broadinstitute.sting.queue.engine.CommandLineJobManager

import java.util.concurrent.{Executors, ExecutorService}

class CMPShellJobManager extends CommandLineJobManager[CMPShellJobRunner] {
  protected var pool: ExecutorService = null

  var creationCount : Int = 0;

  def runnerType = classOf[CMPShellJobRunner]
  def create(function: CommandLineFunction) = {
     creationCount += 1
     new CMPShellJobRunner(function, pool, creationCount)
  }

  override def init() {
     creationCount = 0;
     pool = Executors.newFixedThreadPool( 64)
  }

  override def exit() {
     pool.shutdown()
  }

  override def updateStatus(runners: Set[CMPShellJobRunner]) = {
    var updatedRunners = Set.empty[CMPShellJobRunner]
    runners.foreach(runner => if (runner.updateJobStatus()) {updatedRunners += runner})
    updatedRunners
  }

  override def tryStop(runners: Set[CMPShellJobRunner]) { runners.foreach(_.tryStop()) }
}
