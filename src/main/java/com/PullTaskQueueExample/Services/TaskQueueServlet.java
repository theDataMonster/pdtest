package com.PullTaskQueueExample.Services;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;

@SuppressWarnings("serial")
@WebServlet(
		name="TaskPull",
		description="Task Queue to process some queues",
		urlPatterns="/taskqueues/queue"
		)
public class TaskQueueServlet extends HttpServlet{
	
	private static final Logger log= Logger.getLogger(TaskQueueServlet.class.getName());
	private static final int numberOfTasksToAdd=100;
	private static final int numberOfTasksToLease=100;
	private static boolean useTaggedTasks=true;
	private static String output;
	private static String message;
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException
	{
		if(req.getParameter("addTask")!=null)
		{
			String content=req.getParameter("content");
			String output= String.format( "Adding %d Tasks to the Task Queue with a payload of '%s'",
		              numberOfTasksToAdd, content.toString());
			log.info(output.toString());
			
			Queue q=QueueFactory.getQueue("pull-queue");
			
			if(!useTaggedTasks)
			{
				for(int i=0;i<numberOfTasksToAdd;i++)
				{
					q.add(TaskOptions.Builder.withMethod(TaskOptions.Method.PULL).payload(content.toString()));
				}
			}
			else
			{
				for(int i=0;i<numberOfTasksToAdd;i++)
				{
					q.add(TaskOptions.Builder.withMethod(TaskOptions.Method.PULL).payload(content.toString()).tag("process".getBytes()));
					
					
				}
			}
			
			try
			{
				message="Added "+numberOfTasksToAdd+" tasks added to Task Queue";
				req.setAttribute("message", message);
				req.getRequestDispatcher("taskqueues-pull.jsp").forward(req, resp);
			}
			catch(ServletException e)
			{
				throw new ServletException("Servlet Exception Error",e);
			}
		}
		else
		{
			if(req.getParameter("leaseTask")!=null)
			{
				output=String.format("Pulling %d tasks from the task queue", numberOfTasksToLease);
				log.info(output.toString());
				
				Queue q=QueueFactory.getQueue("pull-queue");
				if(!useTaggedTasks)
				{
					List<TaskHandle> tasks=q.leaseTasks(3600,TimeUnit.SECONDS,numberOfTasksToLease);
					
					message=processTasks(tasks,q);
				}
				else
				{
					List<TaskHandle> tasks=q.leaseTasksByTag(3600, TimeUnit.SECONDS, numberOfTasksToLease, "process");
					message=processTasks(tasks,q);
				}
				req.setAttribute("message", message);
				req.getRequestDispatcher("taskqueues-pull.jsp").forward(req, resp);
			}
			else
			{
				resp.sendRedirect("/");
			}
		}
	}
	
	private static String processTasks(List<TaskHandle> tasks, Queue q)
	{
		String payload;
		int numberOfDeletedTasks=0;
		
		for(TaskHandle task:tasks)
		{
			payload=new String(task.getPayload());
			output=String.format( "Processing: taskName='%s'  payload='%s'",
		              task.getName().toString(), payload.toString());
			
			log.info(output.toString());
			q.deleteTask(task);
			
			numberOfDeletedTasks++;
		}
		
		if(numberOfDeletedTasks>0)
		{
			message="Processed and deleted "+numberOfTasksToLease+" tasks from the task queue";
		}
		else
		{
			message="Task Queue has not available tasks to lease";
		}
		return message;
	}
	

}
