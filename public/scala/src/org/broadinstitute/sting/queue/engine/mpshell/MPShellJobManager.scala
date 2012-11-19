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

class MPShellJobManager extends CommandLineJobManager[MPShellJobRunner] {
  protected var pool: ExecutorService = null

  def runnerType = classOf[MPShellJobRunner]
  def create(function: CommandLineFunction) = new MPShellJobRunner(function, pool)

  override def init() {
     pool = Executors.newFixedThreadPool( 8)
  }

  override def exit() {
     pool.shutdown()
  }

  override def updateStatus(runners: Set[MPShellJobRunner]) = {
    var updatedRunners = Set.empty[MPShellJobRunner]
    runners.foreach(runner => if (runner.updateJobStatus()) {updatedRunners += runner})
    updatedRunners
  }

  override def tryStop(runners: Set[MPShellJobRunner]) { runners.foreach(_.tryStop()) }
}
